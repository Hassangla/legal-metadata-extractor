// ── KIMI SEARCH LOOP ────────────────────────────────────────
// Implements Kimi/Moonshot's echo-based tool-call protocol for $web_search.
// Kimi uses multi-turn Chat Completions: the client must echo each $web_search
// tool call back so Moonshot's server actually performs the search.
//
// NOTE: This file is intentionally standalone — all helper functions it
// depends on (fetchWithRetry, extractUrlsFromText, collectUrlsDeep,
// parseKimiToolCallsFromText) are inlined below to avoid cross-file imports.

export async function runKimiSearchLoop(url, init, inputTokens, outputTokens, kimiObservedToolUrls, kimiObservedToolCalls, _providerType) {
    const MAX_TOOL_ROUNDS = 10;
    const MAX_RETRIES = 3;
    const RETRY_BASE_MS = 2000;
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    // Inline fetchWithRetry to avoid cross-file import
    async function fetchWithRetry(u, i, retries) {
        retries = retries || MAX_RETRIES;
        for (let attempt = 0; attempt <= retries; attempt++) {
            let resp;
            try { resp = await fetch(u, i); } catch (networkErr) {
                if (attempt < retries) { await sleep(RETRY_BASE_MS * Math.pow(2, attempt) + Math.random() * 500); continue; }
                throw new Error(`Network error: ${networkErr.message}`);
            }
            if (resp.ok) return resp;
            if ((resp.status === 429 || resp.status >= 500) && attempt < retries) {
                await sleep(RETRY_BASE_MS * Math.pow(2, attempt) + Math.random() * 500); continue;
            }
            const errText = await resp.text();
            throw new Error(`API ${resp.status}: ${errText.slice(0, 300)}`);
        }
        throw new Error('Exhausted retries');
    }

    // Inline URL helpers
    function extractUrlsFromText(raw) {
        if (!raw || typeof raw !== 'string') return [];
        return (raw.match(/https?:\/\/[^\s)\]}>"'`]+/gi)||[]).map(u=>u.replace(/[.,;:!?]+$/g,'')).filter(u=>u.startsWith('http'));
    }

    function collectUrlsDeep(value, out) {
        if (!value) return;
        if (typeof value === 'string') { for (const u of extractUrlsFromText(value)) out.push(u); return; }
        if (Array.isArray(value)) { for (const v of value) collectUrlsDeep(v, out); return; }
        if (typeof value === 'object') {
            for (const [k, v] of Object.entries(value)) {
                const key = k.toLowerCase();
                if ((key === 'url' || key === 'uri' || key === 'href' || key === 'link') && typeof v === 'string') { out.push(v); continue; }
                collectUrlsDeep(v, out);
            }
        }
    }

    // Inline Kimi embedded tool-call parser
    function parseKimiToolCallsFromText(text) {
        if (!text || !text.includes('<|tool_calls_section_begin|>')) return [];
        const sectionMatch = text.match(/<\|tool_calls_section_begin\|>([\s\S]*?)<\|tool_calls_section_end\|>/);
        if (!sectionMatch) return [];
        const section = sectionMatch[1];
        const toolCalls = [];
        const re = /<\|tool_call_begin\|>\s*functions\.([^:]+):(\S+)\s*<\|tool_call_argument_begin\|>([\s\S]*?)<\|tool_call_end\|>/g;
        let m;
        while ((m = re.exec(section)) !== null) {
            toolCalls.push({ id: m[2].trim(), type: 'function', function: { name: m[1].trim(), arguments: m[3].trim() } });
        }
        return toolCalls;
    }

    const bodyObj = JSON.parse(init.body);
    let messages = [...bodyObj.messages];
    let data;

    for (let round = 0; round < MAX_TOOL_ROUNDS; round++) {
        const reqBody = { ...bodyObj, messages };
        const resp = await fetchWithRetry(url, {
            method: 'POST',
            headers: init.headers,
            body: JSON.stringify(reqBody),
        });
        data = await resp.json();

        if (data.usage) {
            inputTokens += data.usage.prompt_tokens || data.usage.input_tokens || 0;
            outputTokens += data.usage.completion_tokens || data.usage.output_tokens || 0;
        }

        const msg = data.choices?.[0]?.message;
        const finishReason = data.choices?.[0]?.finish_reason;

        if (!msg?.tool_calls || msg.tool_calls.length === 0 || finishReason === 'stop') {
            break;
        }

        kimiObservedToolCalls = true;

        for (const tc of msg.tool_calls) {
            const args = tc.function?.arguments || '';
            const foundUrls = extractUrlsFromText(args);
            for (const u of foundUrls) {
                if (!kimiObservedToolUrls.includes(u)) kimiObservedToolUrls.push(u);
            }
            try {
                const parsed = JSON.parse(args);
                const deepUrls = [];
                collectUrlsDeep(parsed, deepUrls);
                for (const u of deepUrls) {
                    if (!kimiObservedToolUrls.includes(u)) kimiObservedToolUrls.push(u);
                }
            } catch (_) {}
        }

        // Echo assistant message with tool_calls back, then append tool results
        messages.push(msg);
        for (const tc of msg.tool_calls) {
            messages.push({
                role: 'tool',
                tool_call_id: tc.id,
                content: tc.function?.arguments || '{}',
            });
        }
    }

    // Also check for embedded tool-call tokens in final response text
    const finalMsg = data?.choices?.[0]?.message;
    if (finalMsg?.content && typeof finalMsg.content === 'string') {
        const embeddedCalls = parseKimiToolCallsFromText(finalMsg.content);
        if (embeddedCalls.length > 0) {
            kimiObservedToolCalls = true;
            for (const ec of embeddedCalls) {
                const foundUrls = extractUrlsFromText(ec.function?.arguments || '');
                for (const u of foundUrls) {
                    if (!kimiObservedToolUrls.includes(u)) kimiObservedToolUrls.push(u);
                }
            }
        }
    }

    return { data, inputTokens, outputTokens, kimiObservedToolUrls, kimiObservedToolCalls };
}