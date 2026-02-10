import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

const BATCH_SIZE = 5;
const MAX_RETRIES = 3;
const RETRY_BASE_MS = 2000;

// ── PROVIDER CHAT CONFIGS ───────────────────────────────────

const CHAT_CONFIGS = {
    openai:           { chatUrl: (b) => `${b}/v1/chat/completions`, responsesUrl: (b) => `${b}/v1/responses`, authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    openrouter:       { chatUrl: (b) => `${b}/v1/chat/completions`, responsesUrl: (b) => `${b}/v1/responses`, authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    groq:             { chatUrl: (b) => `${b}/openai/v1/chat/completions`,   authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    together:         { chatUrl: (b) => `${b}/v1/chat/completions`,          authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    mistral:          { chatUrl: (b) => `${b}/v1/chat/completions`,          authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    perplexity:       { chatUrl: (b) => `${b}/chat/completions`,             authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    openai_compatible:{ chatUrl: (b) => `${b}/v1/chat/completions`,          authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    azure_openai:     { chatUrl: (b, m) => `${b}/openai/deployments/${m}/chat/completions?api-version=2024-10-21`, authHeaders: (k) => ({ 'api-key': k, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    anthropic:        { chatUrl: (b) => `${b}/v1/messages`,                  authHeaders: (k) => ({ 'x-api-key': k, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' }), chatFormat: 'anthropic' },
    google:           { chatUrl: (b, m) => `${b}/v1beta/models/${m}:generateContent`, authHeaders: (_) => ({ 'Content-Type': 'application/json' }), chatFormat: 'google' },
};

// Providers whose web search is handled server-side (single API call returns search results).
// Only these providers can actually perform web search — all others would need
// client-side search execution, which we do not support.
const SERVER_SIDE_SEARCH = new Set([
    'web_search',          // Anthropic — server-side tool
    'web_search_preview',  // OpenAI — server-side tool (via Responses API)
    'google_search',       // Google Gemini — server-side tool
    'builtin',             // Perplexity — all models search automatically
    'kimi_web_search',     // Kimi/Moonshot — server-side builtin_function (echo-loop protocol)
]);

// ── ENCRYPTION ──────────────────────────────────────────────

function getEncryptionKey() {
    const key = Deno.env.get("ENCRYPTION_KEY");
    if (!key) throw new Error("ENCRYPTION_KEY not set");
    return key;
}

async function deriveKey(secret) {
    const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(secret));
    return crypto.subtle.importKey("raw", hash, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

async function decryptString(ciphertext) {
    if (!ciphertext.includes(".")) { try { return atob(ciphertext); } catch { throw new Error("Invalid key format"); } }
    const key = await deriveKey(getEncryptionKey());
    const [ivB64, cipherB64] = ciphertext.split(".");
    const iv          = Uint8Array.from(atob(ivB64),     c => c.charCodeAt(0));
    const cipherBytes = Uint8Array.from(atob(cipherB64), c => c.charCodeAt(0));
    return new TextDecoder().decode(await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, cipherBytes));
}

// ── BUILD LLM REQUEST (provider-specific) ───────────────────

function buildLLMRequest(providerType, modelId, systemPrompt, userPrompt, webSearchChoice, baseUrl, apiKey) {
    const cfg = CHAT_CONFIGS[providerType] || CHAT_CONFIGS.openai_compatible;

    if (cfg.chatFormat === 'anthropic') {
        const body = {
            model: modelId,
            system: systemPrompt,
            messages: [{ role: 'user', content: userPrompt }],
            max_tokens: 4096,
            temperature: 0,
        };
        if (webSearchChoice === 'web_search') {
            body.tools = [{ type: 'web_search_20250305', name: 'web_search' }];
        }
        return {
            url: cfg.chatUrl(baseUrl, modelId),
            init: { method: 'POST', headers: cfg.authHeaders(apiKey), body: JSON.stringify(body) },
        };
    }

    if (cfg.chatFormat === 'google') {
        const body = {
            contents: [{ role: 'user', parts: [{ text: `${systemPrompt}\n\n${userPrompt}` }] }],
            generationConfig: { temperature: 0, maxOutputTokens: 4096 },
        };
        if (webSearchChoice === 'google_search') {
            body.tools = [{ googleSearch: {} }];
        }
        const url = `${cfg.chatUrl(baseUrl, modelId)}?key=${apiKey}`;
        return {
            url,
            init: { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) },
        };
    }

    // OpenAI / OpenAI-compatible (default)

    if (webSearchChoice === 'web_search_preview') {
        const id = (modelId || '').toLowerCase();

        // Path A: Dedicated search models → Chat Completions + web_search_options
        // These models are specifically trained for search and use Chat Completions.
        const isSearchModel = id.includes('search-preview') || id.includes('search-api');

        if (isSearchModel) {
            const body = {
                model: modelId,
                messages: [
                    { role: 'system', content: systemPrompt },
                    { role: 'user', content: userPrompt },
                ],
                web_search_options: {},
                max_tokens: 4096,
                temperature: 0,
            };
            return {
                url: cfg.chatUrl(baseUrl, modelId),
                init: { method: 'POST', headers: cfg.authHeaders(apiKey), body: JSON.stringify(body) },
                isResponsesApi: false,
            };
        }

        // Path B: All other models → Responses API + web_search tool
        // The Responses API allows any capable model to use web search as a tool.
        if (cfg.responsesUrl) {
            const body = {
                model: modelId,
                instructions: systemPrompt,
                input: userPrompt,
                tools: [{ type: 'web_search' }],
                temperature: 0,
                max_output_tokens: 4096,
                store: false,  // Avoid storage issues with some API plans
            };
            return {
                url: cfg.responsesUrl(baseUrl),
                init: { method: 'POST', headers: cfg.authHeaders(apiKey), body: JSON.stringify(body) },
                isResponsesApi: true,
            };
        }

        // Fallback: provider has no responsesUrl (shouldn't happen for openai/openrouter)
        // Fall through to standard Chat Completions without search
    }

    // Standard Chat Completions path (no web search, Kimi search, Perplexity builtin, or fallback)
    const body = {
        model: modelId,
        messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: userPrompt },
        ],
        max_tokens: 4096,
        temperature: 0,
    };

    // Kimi server-side web search uses builtin_function tool in Chat Completions
    if (webSearchChoice === 'kimi_web_search') {
        body.tools = [{ type: 'builtin_function', function: { name: '$web_search' } }];
    } else if (webSearchChoice === 'none' || !webSearchChoice) {
        // No web search — safe to use response_format for reliable JSON
        body.response_format = { type: 'json_object' };
    }
    // Note: 'builtin' (Perplexity) needs no tools array — search is automatic.

    return {
        url: cfg.chatUrl(baseUrl, modelId),
        init: { method: 'POST', headers: cfg.authHeaders(apiKey), body: JSON.stringify(body) },
        isResponsesApi: false,
    };
}

// ── PARSE LLM RESPONSE ─────────────────────────────────────

function extractTextContent(providerType, data, isResponsesApi) {
    // OpenAI Responses API format: output is an array of items
    if (isResponsesApi) {
        const output = data.output;
        if (Array.isArray(output)) {
            const textParts = [];
            for (const item of output) {
                if (item.type === 'message' && Array.isArray(item.content)) {
                    for (const part of item.content) {
                        if ((part.type === 'output_text' || part.type === 'text') && part.text) {
                            textParts.push(part.text);
                        }
                    }
                }
            }
            if (textParts.length > 0) return textParts.join('\n');
        }
        // Fallback: check if there's a top-level output_text
        if (data.output_text) return data.output_text;
        return '';
    }

    const cfg = CHAT_CONFIGS[providerType] || CHAT_CONFIGS.openai_compatible;

    if (cfg.chatFormat === 'anthropic') {
        return (data.content || [])
            .filter((b) => b.type === 'text')
            .map((b) => b.text)
            .join('\n');
    }
    if (cfg.chatFormat === 'google') {
        return data.candidates?.[0]?.content?.parts
            ?.map((p) => p.text || '')
            .join('\n') || '';
    }

    // OpenAI / OpenAI-compatible format
    const msg = data.choices?.[0]?.message;
    if (!msg) return '';

    // Standard case: content is a string
    if (typeof msg.content === 'string' && msg.content.length > 0) {
        return msg.content;
    }

    // Some providers return content as an array of objects (e.g., web search annotations)
    if (Array.isArray(msg.content)) {
        const textParts = msg.content
            .filter((part) => part.type === 'text' || part.type === 'output_text')
            .map((part) => part.text || '');
        if (textParts.length > 0) return textParts.join('\n');
    }

    // OpenAI web_search_preview: response text is in choices[0].output array
    const output = data.choices?.[0]?.output;
    if (Array.isArray(output)) {
        const textParts = [];
        for (const item of output) {
            if (item.type === 'message' && Array.isArray(item.content)) {
                for (const part of item.content) {
                    if ((part.type === 'output_text' || part.type === 'text') && part.text) {
                        textParts.push(part.text);
                    }
                }
            }
        }
        if (textParts.length > 0) return textParts.join('\n');
    }

    // Fallback: if content is null but tool_calls exist (e.g., wrong tool format),
    // try to extract any useful text from tool call arguments
    if (msg.tool_calls && msg.tool_calls.length > 0) {
        for (let i = msg.tool_calls.length - 1; i >= 0; i--) {
            const tc = msg.tool_calls[i];
            const args = tc.function?.arguments || tc.arguments || '';
            if (args.includes('"output"') || args.includes('"evidence"')) {
                return args;
            }
        }
    }

    if (msg.refusal) return '';
    return '';
}

// ── RETRY WITH BACKOFF ──────────────────────────────────────

async function fetchWithRetry(url, init, retries) {
    retries = retries || MAX_RETRIES;
    for (let attempt = 0; attempt <= retries; attempt++) {
        const resp = await fetch(url, init);
        if (resp.ok) return resp;
        if (resp.status === 429 || resp.status >= 500) {
            if (attempt < retries) {
                const delay = RETRY_BASE_MS * Math.pow(2, attempt) + Math.random() * 500;
                await new Promise(r => setTimeout(r, delay));
                continue;
            }
        }
        const errText = await resp.text();
        throw new Error(`API ${resp.status}: ${errText.slice(0, 300)}`);
    }
    throw new Error('Exhausted retries');
}

// ── JSON EXTRACTION ─────────────────────────────────────────

function extractJSON(content) {
    try { return JSON.parse(content.trim()); } catch (_) {}
    const fenceMatch = content.match(/```(?:json)?\s*([\s\S]*?)```/);
    if (fenceMatch) try { return JSON.parse(fenceMatch[1].trim()); } catch (_) {}
    const braceMatch = content.match(/\{[\s\S]*\}/);
    if (braceMatch) try { return JSON.parse(braceMatch[0].trim()); } catch (_) {}
    return null;
}

// ── ECONOMY ALIASES ─────────────────────────────────────────
// Common alternate names/spellings for economies
const ECONOMY_ALIASES = {
    'ivory coast': "Côte d'Ivoire",
    'cote divoire': "Côte d'Ivoire",
    'cote d ivoire': "Côte d'Ivoire",
    'south korea': 'Korea, Rep.',
    'republic of korea': 'Korea, Rep.',
    'north korea': "Korea, Dem. People's Rep.",
    'democratic republic of the congo': 'Congo, Dem. Rep.',
    'drc': 'Congo, Dem. Rep.',
    'republic of congo': 'Congo, Rep.',
    'czech republic': 'Czechia',
    'swaziland': 'Eswatini',
    'burma': 'Myanmar',
    'holland': 'Netherlands',
    'usa': 'United States',
    'united states of america': 'United States',
    'uk': 'United Kingdom',
    'great britain': 'United Kingdom',
    'russia': 'Russian Federation',
    'iran': 'Iran, Islamic Rep.',
    'syria': 'Syrian Arab Republic',
    'venezuela': 'Venezuela, RB',
    'egypt': 'Egypt, Arab Rep.',
    'yemen': 'Yemen, Rep.',
    'laos': 'Lao PDR',
    'slovakia': 'Slovak Republic',
    'macedonia': 'North Macedonia',
    'cape verde': 'Cabo Verde',
    'east timor': 'Timor-Leste',
    'gambia': 'Gambia, The',
    'bahamas': 'Bahamas, The',
    'taiwan': 'Taiwan, China',
    'hong kong': 'Hong Kong SAR, China',
    'macau': 'Macao SAR, China',
    'macao': 'Macao SAR, China',
    'palestine': 'West Bank and Gaza',
    'brunei': 'Brunei Darussalam',
    'micronesia': 'Micronesia, Fed. Sts.',
    'vietnam': 'Viet Nam',
    'kyrgyzstan': 'Kyrgyz Republic',
    'st. lucia': 'St. Lucia',
    'saint lucia': 'St. Lucia',
    'st. kitts': 'St. Kitts and Nevis',
    'saint kitts': 'St. Kitts and Nevis',
    'st. vincent': 'St. Vincent and the Grenadines',
    'saint vincent': 'St. Vincent and the Grenadines',
};

// ── MODEL PRICING (per million tokens) ──────────────────────
const MODEL_PRICING = {
    // OpenAI
    'gpt-4o':              { input: 2.50,  output: 10.00 },
    'gpt-4o-mini':         { input: 0.15,  output: 0.60 },
    'gpt-4o-search-preview': { input: 2.50, output: 10.00 },
    'gpt-4-turbo':         { input: 10.00, output: 30.00 },
    'gpt-4.1':             { input: 2.00,  output: 8.00 },
    'gpt-4.1-mini':        { input: 0.40,  output: 1.60 },
    'gpt-4.1-nano':        { input: 0.10,  output: 0.40 },
    'gpt-4.5-preview':     { input: 75.00, output: 150.00 },
    'gpt-3.5-turbo':       { input: 0.50,  output: 1.50 },
    'chatgpt-4o-latest':   { input: 5.00,  output: 15.00 },
    'o1':                  { input: 15.00, output: 60.00 },
    'o1-mini':             { input: 1.10,  output: 4.40 },
    'o1-preview':          { input: 15.00, output: 60.00 },
    'o3':                  { input: 2.00,  output: 8.00 },
    'o3-mini':             { input: 1.10,  output: 4.40 },
    'o4-mini':             { input: 1.10,  output: 4.40 },
    // Anthropic
    'claude-sonnet-4':     { input: 3.00,  output: 15.00 },
    'claude-opus-4':       { input: 15.00, output: 75.00 },
    'claude-haiku-3.5':    { input: 0.80,  output: 4.00 },
    'claude-3-5-sonnet':   { input: 3.00,  output: 15.00 },
    'claude-3-5-haiku':    { input: 0.80,  output: 4.00 },
    'claude-3-opus':       { input: 15.00, output: 75.00 },
    // Google
    'gemini-2.5-pro':      { input: 1.25,  output: 10.00 },
    'gemini-2.5-flash':    { input: 0.15,  output: 0.60 },
    'gemini-2.0-flash':    { input: 0.10,  output: 0.40 },
    'gemini-1.5-pro':      { input: 1.25,  output: 5.00 },
    'gemini-1.5-flash':    { input: 0.075, output: 0.30 },
    // Moonshot / Kimi
    'moonshot-v1-auto':    { input: 0.55,  output: 0.55 },
    'moonshot-v1-8k':      { input: 0.17,  output: 0.17 },
    'moonshot-v1-32k':     { input: 0.33,  output: 0.33 },
    'moonshot-v1-128k':    { input: 0.83,  output: 0.83 },
    'kimi-latest':         { input: 0.55,  output: 0.55 },
    // DeepSeek
    'deepseek-chat':       { input: 0.14,  output: 0.28 },
    'deepseek-reasoner':   { input: 0.55,  output: 2.19 },
    // Perplexity
    'sonar':               { input: 1.00,  output: 1.00 },
    'sonar-pro':           { input: 3.00,  output: 15.00 },
    'sonar-reasoning':     { input: 1.00,  output: 5.00 },
    'sonar-reasoning-pro': { input: 2.00,  output: 8.00 },
    // xAI
    'grok-3':              { input: 3.00,  output: 15.00 },
    'grok-3-mini':         { input: 0.30,  output: 0.50 },
    'grok-2':              { input: 2.00,  output: 10.00 },
    // Mistral
    'mistral-large':       { input: 2.00,  output: 6.00 },
    'mistral-small':       { input: 0.10,  output: 0.30 },
};

function estimateCostFromPricing(inputPricePerMillion, outputPricePerMillion, inputTokens, outputTokens) {
    return ((inputTokens * inputPricePerMillion) + (outputTokens * outputPricePerMillion)) / 1_000_000;
}

function estimateCostFromTable(modelId, inputTokens, outputTokens) {
    const id = (modelId || '').toLowerCase();

    if (MODEL_PRICING[id]) {
        const p = MODEL_PRICING[id];
        return ((inputTokens * p.input) + (outputTokens * p.output)) / 1_000_000;
    }

    const sortedEntries = Object.entries(MODEL_PRICING).sort(([a], [b]) => b.length - a.length);
    for (const [key, p] of sortedEntries) {
        if (id.startsWith(key) || id.includes(key)) {
            return ((inputTokens * p.input) + (outputTokens * p.output)) / 1_000_000;
        }
    }

    return ((inputTokens * 2) + (outputTokens * 8)) / 1_000_000;
}

// ── MAIN HANDLER ────────────────────────────────────────────

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        if (!user) return Response.json({ error: 'Unauthorized' }, { status: 401 });

        const { action, ...params } = await req.json();

        switch (action) {

            case 'create': {
                const { connection_id, model_id, web_search_choice, input_file_url, input_file_name, total_rows, input_rows, task_name } = params;

                const specs = await base44.entities.Spec.filter({ is_active: true });
                if (!specs.length) return Response.json({ error: 'No active spec. Configure the spec first.' }, { status: 400 });

                const versions = await base44.entities.SpecVersion.filter({ spec_id: specs[0].id });
                if (!versions.length) return Response.json({ error: 'No spec version found.' }, { status: 400 });
                versions.sort((a, b) => (b.version_number || 0) - (a.version_number || 0));
                const latestVersion = versions[0];

                const connections = await base44.entities.APIConnection.filter({ id: connection_id });
                const conn = connections[0];
                const models = await base44.entities.ModelCatalog.filter({ connection_id, model_id });
                const model = models[0];

                const job = await base44.entities.Job.create({
                    connection_id,
                    model_id,
                    web_search_choice: web_search_choice || 'none',
                    spec_version_id: latestVersion.id,
                    status: 'queued',
                    input_file_url,
                    input_file_name,
                    total_rows: total_rows || 0,
                    processed_rows: 0,
                    progress_json: { current_batch: 0, last_row_index: 0 },
                    connection_name: conn?.name || 'Unknown',
                    model_name: model?.display_name || model_id,
                    provider_type: conn?.provider_type || 'openai_compatible',
                    task_name: task_name || '',
                    total_input_tokens: 0,
                    total_output_tokens: 0,
                    estimated_cost_usd: 0,
                });

                if (input_rows?.length) {
                    for (let i = 0; i < input_rows.length; i++) {
                        await base44.entities.JobRow.create({
                            job_id: job.id,
                            row_index: i + 1,
                            input_data: input_rows[i],
                            status: 'pending',
                        });
                    }
                }

                return Response.json({ job });
            }

            case 'process': {
                const { job_id } = params;
                const jobs = await base44.entities.Job.filter({ id: job_id });
                if (!jobs.length) return Response.json({ error: 'Job not found' }, { status: 404 });

                const job = jobs[0];
                if (job.status === 'done' || job.status === 'error') {
                    return Response.json({ job, message: 'Job already completed' });
                }

                await base44.entities.Job.update(job_id, { status: 'running' });

                const connections = await base44.entities.APIConnection.filter({ id: job.connection_id });
                if (!connections.length) {
                    await base44.entities.Job.update(job_id, { status: 'error', error_message: 'API connection not found' });
                    return Response.json({ error: 'Connection not found' }, { status: 404 });
                }
                const conn = connections[0];
                const apiKey = await decryptString(conn.api_key_encrypted);
                const providerType = conn.provider_type || job.provider_type || 'openai_compatible';

                const specVersions = await base44.entities.SpecVersion.filter({ id: job.spec_version_id });
                const specText = specVersions[0]?.spec_text || '';

                const economyCodes = await base44.entities.EconomyCode.list();
                const economyMap = {};
                economyCodes.forEach((ec) => { economyMap[ec.economy.toLowerCase().trim()] = ec.economy_code; });

                // Look up stored model pricing from ModelCatalog
                let modelInputPrice = 0;
                let modelOutputPrice = 0;
                try {
                    const catalogEntries = await base44.entities.ModelCatalog.filter({
                        connection_id: job.connection_id,
                        model_id: job.model_id,
                    });
                    if (catalogEntries.length > 0 && catalogEntries[0].input_price_per_million > 0) {
                        modelInputPrice = catalogEntries[0].input_price_per_million;
                        modelOutputPrice = catalogEntries[0].output_price_per_million || 0;
                    }
                } catch (_) {}

                // Use smaller batch size when web search is enabled to avoid serverless timeout.
                // Web search calls take 10-20s each; with batch=5 that's 50-100s which exceeds
                // typical serverless timeouts (30-60s).
                const hasWebSearch = job.web_search_choice && job.web_search_choice !== 'none';
                const effectiveBatchSize = hasWebSearch ? 2 : BATCH_SIZE;

                const allRows = await base44.entities.JobRow.filter({ job_id });
                const pendingRows = allRows
                    .filter((r) => r.status === 'pending')
                    .sort((a, b) => a.row_index - b.row_index)
                    .slice(0, effectiveBatchSize);

                if (!pendingRows.length) {
                    await base44.entities.Job.update(job_id, { status: 'done', processed_rows: job.total_rows });
                    return Response.json({ job: { ...job, status: 'done' }, message: 'All rows processed' });
                }

                let processedCount = 0;

                for (const row of pendingRows) {
                    try {
                        await base44.entities.JobRow.update(row.id, { status: 'processing' });
                        const input = row.input_data;
                        const rawEconomy = input.Economy?.toLowerCase()?.trim() || '';
                        const resolvedEconomy = ECONOMY_ALIASES[rawEconomy] || rawEconomy;
                        const economyCode = economyMap[resolvedEconomy] || economyMap[rawEconomy] || '';
                        const legalBasis = input.Legal_basis || input['Legal basis'] || '';

                        // Spec-compliant 3-attempt search strategy
                        const query1 = `"${legalBasis}" "${input.Economy}" (law OR act OR code OR decree OR regulation)`;
                        const query2 = `"${legalBasis}" "${input.Economy}" (official gazette OR ministry of justice OR parliament OR government)`;
                        const query3 = legalBasis
                            ? `"${legalBasis}" "${input.Economy}" ("Law No" OR "Act No" OR "Decree No" OR "gazette" OR "promulgated" OR "entered into force")`
                            : `"${input.Topic}" "${input.Economy}" "${input.Question}" legal instrument`;

                        // Determine if we have REAL server-side web search
                        const hasRealWebSearch = job.web_search_choice
                            && job.web_search_choice !== 'none'
                            && SERVER_SIDE_SEARCH.has(job.web_search_choice);

                        // If user selected a non-server-side search tool, fall back to no search
                        const effectiveWebSearch = hasRealWebSearch ? job.web_search_choice : 'none';

                        // When web search is not available, override the spec's TOOL-DEPENDENT rule
                        // which would force all fields blank. Instead, instruct the LLM to use
                        // its training knowledge.
                        const specOverride = hasRealWebSearch ? '' : `
IMPORTANT OVERRIDE — READ FIRST:
The specification below contains a "TOOL-DEPENDENT" rule that says to return blank fields if no web search tool is available. IGNORE that rule for this request. Instead, use your training knowledge to fill in as many fields as possible. Only leave fields blank if you genuinely do not know the answer. Note "Web search not available — used training knowledge" in Missing_Conflict_Reason.
`;

                        const systemPrompt = `You are a legal-instrument metadata extraction and verification tool. Follow the specification below EXACTLY.
${specOverride}
CRITICAL OUTPUT RULES:
- Respond with valid JSON ONLY.
- Do NOT wrap your response in markdown code fences (\`\`\`json ... \`\`\`).
- Do NOT include any explanation, commentary, or text before or after the JSON.
- The response must start with { and end with }.
- If you cannot find information for a field, leave it as an empty string "".

${specText}`;

                        // Build user prompt — conditional on whether real web search is available
                        let searchInstructions;
                        if (hasRealWebSearch) {
                            searchInstructions = `SEARCH QUERIES (use the web search tool to research; adapt to local language if non-English economy):
1. ${query1}
2. ${query2}
3. ${query3}

INSTRUCTIONS:
1. Search using the queries above. Stop early only if Tier 1-2 sources clearly answer the needed fields.
2. Identify the best source (prefer Tier 1 official government, then Tier 2 legal databases, then Tier 3, etc.).
3. Extract the official title in original language/script. Normalize it per the Title Normalization Rules.
4. Determine Language_Doc (language of official publication), Enactment_Date, Date_of_Entry_in_Force, Current_Status.
5. For Instrument_Published_Name: if Language_Doc is French or Spanish, keep the normalized title as-is (DO NOT translate). Otherwise provide an English name.
6. Record all evidence, URLs considered, tier, and reasoning.`;
                        } else {
                            searchInstructions = `NOTE: Web search is NOT available for this request. Use your training knowledge and any information you know about this legal instrument to extract the metadata as accurately as possible.

REFERENCE QUERIES (for context only — do NOT attempt to call any search tool):
1. ${query1}
2. ${query2}
3. ${query3}

INSTRUCTIONS:
1. Use your training knowledge to identify the legal instrument described above.
2. Extract the official title in original language/script. Normalize it per the Title Normalization Rules.
3. Determine Language_Doc, Enactment_Date, Date_of_Entry_in_Force, Current_Status based on your knowledge.
4. For Instrument_Published_Name: if Language_Doc is French or Spanish, keep the normalized title as-is (DO NOT translate). Otherwise provide an English name.
5. For any field you cannot verify without web search, leave it blank and explain in Missing_Conflict_Reason that web search was not available.
6. Record your reasoning in the evidence fields. For URLs, provide the most likely official source URL if you know it, otherwise leave blank.`;
                        }

                        const userPrompt = `Extract legal instrument metadata for this row.

INPUT DATA:
- Economy: ${input.Economy}
- Economy_Code: ${economyCode}
- Legal basis: ${legalBasis}
- Question: ${input.Question}
- Topic: ${input.Topic}

${searchInstructions}

Return a JSON object with EXACTLY this structure (no extra keys, no missing keys). Return ONLY valid JSON — no markdown fences, no explanation text, no code blocks:
{
  "output": {
    "Economy_Code": "${economyCode}",
    "Economy": "${input.Economy}",
    "Language_Doc": "",
    "Instrument_Full_Name_Original_Language": "",
    "Instrument_Published_Name": "",
    "Instrument_URL": "",
    "Enactment_Date": "",
    "Date_of_Entry_in_Force": "",
    "Repeal_Year": "",
    "Current_Status": "",
    "Public": "",
    "Flag": ""
  },
  "evidence": {
    "Row_Index": ${row.row_index},
    "Query_1": ${JSON.stringify(query1)},
    "Query_2": ${JSON.stringify(query2)},
    "Query_3": ${JSON.stringify(query3)},
    "URLs_Considered": "",
    "Selected_Source_URLs": "",
    "Source_Tier": "",
    "Public_Access": "",
    "Raw_Official_Title_As_Source": "",
    "Normalized_Title_Used": "",
    "Language_Justification": "",
    "Instrument_URL_Support": "",
    "Enactment_Support": "",
    "EntryIntoForce_Support": "",
    "Status_Support": "",
    "Missing_Conflict_Reason": "",
    "Normalization_Notes": ""
  }
}`;

                        const { url, init, isResponsesApi } = buildLLMRequest(
                            providerType, job.model_id, systemPrompt, userPrompt,
                            effectiveWebSearch, conn.base_url, apiKey
                        );

                        let data;
                        let inputTokens = 0;
                        let outputTokens = 0;

                        // Kimi uses an echo-based tool-call loop: the client echoes
                        // $web_search arguments back, and Moonshot's server performs
                        // the actual search on the next round-trip.
                        const isKimiSearch = effectiveWebSearch === 'kimi_web_search';

                        if (isKimiSearch) {
                            const bodyObj = JSON.parse(init.body);
                            const MAX_TOOL_LOOPS = 10;

                            for (let loop = 0; loop <= MAX_TOOL_LOOPS; loop++) {
                                const loopResp = await fetchWithRetry(url, {
                                    method: 'POST',
                                    headers: init.headers,
                                    body: JSON.stringify(bodyObj),
                                });
                                data = await loopResp.json();

                                if (data.usage) {
                                    inputTokens += data.usage.prompt_tokens || data.usage.input_tokens || 0;
                                    outputTokens += data.usage.completion_tokens || data.usage.output_tokens || 0;
                                }

                                const choice = data.choices?.[0];
                                if (!choice) break;
                                if (choice.finish_reason === 'stop') break;

                                // If model returned content alongside tool_calls, use it
                                if (choice.message?.content &&
                                    typeof choice.message.content === 'string' &&
                                    choice.message.content.length > 20) {
                                    break;
                                }

                                // Kimi echo protocol: append assistant tool_calls, then
                                // echo each tool's arguments back as the tool result.
                                // Moonshot's server recognizes $web_search and performs
                                // the actual search, returning results on the next call.
                                if (choice.finish_reason === 'tool_calls' && choice.message?.tool_calls?.length > 0) {
                                    bodyObj.messages.push({
                                        role: 'assistant',
                                        content: choice.message.content || null,
                                        tool_calls: choice.message.tool_calls,
                                    });

                                    for (const tc of choice.message.tool_calls) {
                                        bodyObj.messages.push({
                                            role: 'tool',
                                            tool_call_id: tc.id,
                                            content: tc.function?.arguments || JSON.stringify({ status: 'ok' }),
                                        });
                                    }
                                    continue;
                                }

                                break;
                            }
                        } else {
                            // Single-call path: Anthropic, Google, Perplexity, OpenAI Responses API, and no-search
                            const resp = await fetchWithRetry(url, init);
                            data = await resp.json();

                            // Responses API may return 200 with status != 'completed'
                            if (isResponsesApi && data.status === 'failed') {
                                throw new Error(`Responses API failed: ${JSON.stringify(data.error || data.incomplete_details || 'unknown').slice(0, 300)}`);
                            }

                            if (data.usage) {
                                inputTokens = data.usage.prompt_tokens || data.usage.input_tokens || 0;
                                outputTokens = data.usage.completion_tokens || data.usage.output_tokens || 0;
                            } else if (data.usageMetadata) {
                                inputTokens = data.usageMetadata.promptTokenCount || 0;
                                outputTokens = data.usageMetadata.candidatesTokenCount || 0;
                            }
                            // Responses API may report tokens differently
                            if (isResponsesApi && inputTokens === 0 && data.usage) {
                                inputTokens = data.usage.input_tokens || 0;
                                outputTokens = data.usage.output_tokens || 0;
                            }
                        }

                        const content = extractTextContent(providerType, data, isResponsesApi);
                        let parsed = extractJSON(content);

                        // Tag extraction status
                        if (parsed && parsed.output) {
                            parsed.output.Extraction_Status = 'success';
                        }

                        if (!parsed) {
                            const hasToolCalls = !!(data.choices?.[0]?.message?.tool_calls?.length);
                            const rawContent = data.choices?.[0]?.message?.content;
                            const finishReason = data.choices?.[0]?.finish_reason || '';
                            let diagInfo = `Failed to parse LLM response. [web_search=${effectiveWebSearch}, requested=${job.web_search_choice}]`;
                            if (isResponsesApi) {
                                const outputTypes = Array.isArray(data.output) ? data.output.map(i => i.type).join(', ') : 'none';
                                diagInfo += ` [responses_api, output_types=${outputTypes}]`;
                                // Log the actual error if the Responses API returned one
                                if (data.error) {
                                    diagInfo += ` [api_error: ${JSON.stringify(data.error).slice(0, 200)}]`;
                                }
                                if (data.status && data.status !== 'completed') {
                                    diagInfo += ` [status=${data.status}]`;
                                }
                                if (data.incomplete_details) {
                                    diagInfo += ` [incomplete: ${JSON.stringify(data.incomplete_details).slice(0, 200)}]`;
                                }
                            }
                            if (hasToolCalls && !rawContent) {
                                diagInfo += ' [model returned tool_calls with null content — likely wrong web search tool format for this provider]';
                            } else if (!content) {
                                diagInfo += ' [empty content]';
                            } else {
                                diagInfo += ' Raw: ' + (content || '').slice(0, 400);
                            }
                            diagInfo += ` [finish_reason=${finishReason}]`;

                            parsed = {
                                output: {
                                    Economy_Code: economyCode,
                                    Economy: input.Economy,
                                    Language_Doc: '',
                                    Instrument_Full_Name_Original_Language: '',
                                    Instrument_Published_Name: '',
                                    Instrument_URL: '',
                                    Enactment_Date: '',
                                    Date_of_Entry_in_Force: '',
                                    Repeal_Year: '',
                                    Current_Status: '',
                                    Public: '',
                                    Flag: 'No sources',
                                    Extraction_Status: 'parse_error',
                                },
                                evidence: {
                                    Row_Index: row.row_index,
                                    Query_1: query1, Query_2: query2, Query_3: query3,
                                    URLs_Considered: '',
                                    Selected_Source_URLs: '',
                                    Source_Tier: '',
                                    Missing_Conflict_Reason: diagInfo,
                                },
                            };
                        }

                        // Inject server-side values the LLM must not override
                        if (parsed.output) {
                            parsed.output.Economy_Code = economyCode;
                            parsed.output.Economy = input.Economy;
                        }
                        if (parsed.evidence) {
                            parsed.evidence.Row_Index = row.row_index;
                            parsed.evidence.Economy = input.Economy;
                            parsed.evidence.Economy_Code = economyCode;
                            parsed.evidence.Legal_basis_verbatim = legalBasis;
                        }
                        if (!economyCode && parsed.evidence) {
                            const prev = parsed.evidence.Missing_Conflict_Reason || '';
                            parsed.evidence.Missing_Conflict_Reason = [prev, 'Economy code not found in lookup table'].filter(Boolean).join('; ');
                        }

                        await base44.entities.JobRow.update(row.id, {
                            status: 'done',
                            output_json: parsed.output || {},
                            evidence_json: parsed.evidence || {},
                            input_tokens: inputTokens,
                            output_tokens: outputTokens,
                        });
                        processedCount++;

                    } catch (error) {
                        await base44.entities.JobRow.update(row.id, {
                            status: 'error',
                            error_message: error.message?.slice(0, 500),
                        });
                    }
                }

                const updatedRows = await base44.entities.JobRow.filter({ job_id });
                const doneCount  = updatedRows.filter((r) => r.status === 'done').length;
                const errCount   = updatedRows.filter((r) => r.status === 'error').length;
                const pendingLeft = updatedRows.filter((r) => r.status === 'pending').length;
                const newStatus  = pendingLeft === 0 ? 'done' : 'running';

                // Aggregate token usage across all done rows
                const doneRows = updatedRows.filter(r => r.status === 'done');
                const totalInputTokens = doneRows.reduce((sum, r) => sum + (r.input_tokens || 0), 0);
                const totalOutputTokens = doneRows.reduce((sum, r) => sum + (r.output_tokens || 0), 0);

                const updatePayload = {
                    processed_rows: doneCount + errCount,
                    status: newStatus,
                    progress_json: {
                        current_batch: (job.progress_json?.current_batch || 0) + 1,
                        last_row_index: pendingRows[pendingRows.length - 1]?.row_index || 0,
                    },
                    total_input_tokens: totalInputTokens,
                    total_output_tokens: totalOutputTokens,
                };

                // Calculate estimated cost every batch
                if (totalInputTokens > 0 || totalOutputTokens > 0) {
                    let cost;
                    if (modelInputPrice > 0) {
                        cost = estimateCostFromPricing(modelInputPrice, modelOutputPrice, totalInputTokens, totalOutputTokens);
                    } else {
                        cost = estimateCostFromTable(job.model_id, totalInputTokens, totalOutputTokens);
                    }
                    updatePayload.estimated_cost_usd = cost;
                }

                await base44.entities.Job.update(job_id, updatePayload);

                const updatedJobs = await base44.entities.Job.filter({ id: job_id });
                return Response.json({
                    job: updatedJobs[0],
                    processed_this_batch: processedCount,
                    remaining: pendingLeft,
                });
            }

            case 'getStatus': {
                const { job_id } = params;
                const jobs = await base44.entities.Job.filter({ id: job_id });
                if (!jobs.length) return Response.json({ error: 'Job not found' }, { status: 404 });

                const rows = await base44.entities.JobRow.filter({ job_id });
                const statusCounts = {
                    pending:    rows.filter((r) => r.status === 'pending').length,
                    processing: rows.filter((r) => r.status === 'processing').length,
                    done:       rows.filter((r) => r.status === 'done').length,
                    error:      rows.filter((r) => r.status === 'error').length,
                };

                return Response.json({ job: jobs[0], statusCounts });
            }

            case 'list': {
                const jobs = await base44.entities.Job.filter({ created_by: user.email });
                jobs.sort((a, b) => new Date(b.created_date).getTime() - new Date(a.created_date).getTime());
                return Response.json({ jobs });
            }

            case 'rerun': {
                const { job_id, use_latest_spec } = params;
                const oldJobs = await base44.entities.Job.filter({ id: job_id });
                if (!oldJobs.length) return Response.json({ error: 'Original job not found' }, { status: 404 });
                const oldJob = oldJobs[0];

                // Get spec version
                let specVersionId = oldJob.spec_version_id;
                if (use_latest_spec) {
                    const specs = await base44.entities.Spec.filter({ is_active: true });
                    if (specs.length) {
                        const versions = await base44.entities.SpecVersion.filter({ spec_id: specs[0].id });
                        if (versions.length) {
                            versions.sort((a, b) => (b.version_number || 0) - (a.version_number || 0));
                            specVersionId = versions[0].id;
                        }
                    }
                }

                // Get original rows for input data
                const oldRows = await base44.entities.JobRow.filter({ job_id });
                oldRows.sort((a, b) => a.row_index - b.row_index);

                // Create new job
                const newJob = await base44.entities.Job.create({
                    connection_id: oldJob.connection_id,
                    model_id: oldJob.model_id,
                    web_search_choice: oldJob.web_search_choice || 'none',
                    spec_version_id: specVersionId,
                    status: 'queued',
                    input_file_url: oldJob.input_file_url,
                    input_file_name: oldJob.input_file_name,
                    total_rows: oldJob.total_rows,
                    processed_rows: 0,
                    progress_json: { current_batch: 0, last_row_index: 0 },
                    connection_name: oldJob.connection_name,
                    model_name: oldJob.model_name,
                    provider_type: oldJob.provider_type || 'openai_compatible',
                });

                // Create new rows from original input data
                for (const oldRow of oldRows) {
                    await base44.entities.JobRow.create({
                        job_id: newJob.id,
                        row_index: oldRow.row_index,
                        input_data: oldRow.input_data,
                        status: 'pending',
                    });
                }

                return Response.json({ job: newJob });
            }

            case 'getRows': {
                const { job_id } = params;
                const rows = await base44.entities.JobRow.filter({ job_id });
                rows.sort((a, b) => a.row_index - b.row_index);
                return Response.json({ rows });
            }

            case 'stop': {
                const { job_id } = params;
                const jobs = await base44.entities.Job.filter({ id: job_id });
                if (!jobs.length) return Response.json({ error: 'Job not found' }, { status: 404 });

                const job = jobs[0];
                if (job.status !== 'running' && job.status !== 'queued') {
                    return Response.json({ error: 'Job is not running' }, { status: 400 });
                }

                const allRows = await base44.entities.JobRow.filter({ job_id });
                let stopped = 0;
                for (const row of allRows) {
                    if (row.status === 'pending' || row.status === 'processing') {
                        await base44.entities.JobRow.update(row.id, {
                            status: 'error',
                            error_message: 'Stopped by user'
                        });
                        stopped++;
                    }
                }

                const doneRows = allRows.filter(r => r.status === 'done');
                const totalInputTokens = doneRows.reduce((sum, r) => sum + (r.input_tokens || 0), 0);
                const totalOutputTokens = doneRows.reduce((sum, r) => sum + (r.output_tokens || 0), 0);

                // Look up stored pricing for stop cost
                let stopCost;
                try {
                    const catalogEntries = await base44.entities.ModelCatalog.filter({
                        connection_id: job.connection_id,
                        model_id: job.model_id,
                    });
                    if (catalogEntries.length > 0 && catalogEntries[0].input_price_per_million > 0) {
                        stopCost = estimateCostFromPricing(catalogEntries[0].input_price_per_million, catalogEntries[0].output_price_per_million || 0, totalInputTokens, totalOutputTokens);
                    }
                } catch (_) {}
                if (stopCost === undefined) {
                    stopCost = estimateCostFromTable(job.model_id, totalInputTokens, totalOutputTokens);
                }

                await base44.entities.Job.update(job_id, {
                    status: 'done',
                    error_message: `Stopped by user. ${stopped} rows skipped.`,
                    total_input_tokens: totalInputTokens,
                    total_output_tokens: totalOutputTokens,
                    estimated_cost_usd: stopCost,
                });

                return Response.json({ success: true, stopped });
            }

            case 'rename': {
                const { job_id, task_name } = params;
                const jobs = await base44.entities.Job.filter({ id: job_id });
                if (!jobs.length) return Response.json({ error: 'Job not found' }, { status: 404 });

                await base44.entities.Job.update(job_id, { task_name: task_name || '' });
                return Response.json({ success: true });
            }

            default:
                return Response.json({ error: `Unknown action: ${action}` }, { status: 400 });
        }
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});