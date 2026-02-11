import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

// ── Unit-style tests for URL extraction & provenance enforcement ──

// Mock OpenAI Responses API data
const MOCK_OPENAI_RESPONSES = {
    status: 'completed',
    output: [
        {
            type: 'message',
            content: [
                {
                    type: 'output_text',
                    text: 'Some response text',
                    annotations: [
                        { type: 'url_citation', url: 'https://example.com/law1' },
                        { type: 'url_citation', url: 'https://example.com/law2' },
                        { type: 'url_citation', url: 'https://example.com/law1' }, // duplicate
                    ],
                },
            ],
        },
    ],
};

const MOCK_OPENAI_RESPONSES_NO_URLS = {
    status: 'completed',
    output: [
        { type: 'message', content: [{ type: 'output_text', text: 'No search results' }] },
    ],
};

const MOCK_ANTHROPIC_RESPONSE = {
    content: [
        {
            type: 'web_search_tool_result',
            content: [
                { url: 'https://anthropic-source.com/doc1' },
                { url: 'https://anthropic-source.com/doc2' },
            ],
        },
        { type: 'text', text: 'Analysis text here' },
    ],
};

const MOCK_GOOGLE_RESPONSE = {
    candidates: [{
        content: { parts: [{ text: 'response' }] },
        groundingMetadata: {
            groundingChunks: [
                { web: { uri: 'https://google-source.com/law1' } },
                { web: { uri: 'https://google-source.com/law2' } },
            ],
        },
    }],
};

const MOCK_PERPLEXITY_RESPONSE = {
    choices: [{ message: { content: 'Some content' } }],
    citations: [
        'https://perplexity-source.com/doc1',
        'https://perplexity-source.com/doc2',
    ],
};

// Mock OpenAI Chat Completions with url_citation annotations in content array
const MOCK_OPENAI_CHAT_URLCITATIONS = {
    choices: [{
        message: {
            content: [
                {
                    type: 'text',
                    text: 'Here is the analysis.',
                    annotations: [
                        { type: 'url_citation', url: 'https://chat-source.com/law1' },
                        { type: 'url_citation', url: 'https://chat-source.com/law2' },
                    ],
                },
            ],
        },
        finish_reason: 'stop',
    }],
};

// ── Extraction functions (duplicated from jobProcessor for testing) ──

function extractToolUrlsFromResponse(providerType, data, isResponsesApi) {
    const urls = [];
    if (isResponsesApi) {
        if (Array.isArray(data?.output)) {
            for (const item of data.output) {
                if (item.type === 'message' && Array.isArray(item.content)) {
                    for (const part of item.content) {
                        if (part.annotations && Array.isArray(part.annotations)) {
                            for (const ann of part.annotations) {
                                if (ann.type === 'url_citation' && ann.url) urls.push(ann.url);
                            }
                        }
                    }
                }
            }
        }
        return [...new Set(urls)];
    }
    if (providerType === 'anthropic') {
        for (const block of (data?.content || [])) {
            if (block.type === 'web_search_tool_result' && Array.isArray(block.content)) {
                for (const item of block.content) { if (item.url) urls.push(item.url); }
            }
        }
        return [...new Set(urls)];
    }
    if (providerType === 'google') {
        const chunks = data?.candidates?.[0]?.groundingMetadata?.groundingChunks || [];
        for (const chunk of chunks) { if (chunk.web?.uri) urls.push(chunk.web.uri); }
        return [...new Set(urls)];
    }
    if (providerType === 'perplexity') {
        const citations = data?.citations || [];
        for (const c of citations) { if (typeof c === 'string' && c.startsWith('http')) urls.push(c); }
        return [...new Set(urls)];
    }
    // OpenAI Chat Completions web_search_preview: content may be array with url_citation annotations
    const msg = data?.choices?.[0]?.message;
    if (msg && Array.isArray(msg.content)) {
        for (const part of msg.content) {
            if (part.annotations && Array.isArray(part.annotations)) {
                for (const ann of part.annotations) {
                    if (ann.type === 'url_citation' && ann.url) urls.push(ann.url);
                }
            }
        }
    }
    return [...new Set(urls)];
}

function isNoSearchToolError(providerType, data, content, isResponsesApi) {
    if (!content && !data) return false;
    const text = (content || '').toLowerCase();
    if (text.includes('no web search tool') || text.includes('web search is not available') ||
        text.includes('cannot perform web search')) return true;
    if (isResponsesApi && data?.status === 'failed') return true;
    return false;
}

function urlInList(url, list) {
    if (!url || !list) return false;
    const normalized = url.replace(/\/+$/, '').toLowerCase();
    const items = typeof list === 'string' ? list.split(/[,;\n]+/).map(u => u.trim()).filter(u => u.startsWith('http')) : (Array.isArray(list) ? list : []);
    return items.some(item => item.replace(/\/+$/, '').toLowerCase() === normalized);
}

function isSafeHttpUrl(urlStr) {
    if (!urlStr || typeof urlStr !== 'string') return false;
    let parsed;
    try { parsed = new URL(urlStr); } catch (_) { return false; }
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') return false;
    if (parsed.username || parsed.password) return false;
    const hostname = parsed.hostname.toLowerCase();
    if (hostname === 'localhost' || hostname.endsWith('.local')) return false;
    if (hostname === '[::1]' || hostname === '::1') return false;
    const ipv6Bare = hostname.replace(/^\[|\]$/g, '');
    if (/^f[cd]/i.test(ipv6Bare) || /^fe[89ab]/i.test(ipv6Bare)) return false;
    const ipMatch = hostname.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
    if (ipMatch) {
        const [, a, b] = ipMatch.map(Number);
        if (a === 127 || a === 10 || a === 0) return false;
        if (a === 172 && b >= 16 && b <= 31) return false;
        if (a === 192 && b === 168) return false;
        if (a === 169 && b === 254) return false;
    }
    return true;
}

// ── Test Runner ──

function assert(condition, testName) {
    if (!condition) throw new Error(`FAIL: ${testName}`);
    return `PASS: ${testName}`;
}

function runTests() {
    const results = [];

    // Test 1: OpenAI Responses API URL extraction
    const openaiUrls = extractToolUrlsFromResponse('openai', MOCK_OPENAI_RESPONSES, true);
    results.push(assert(openaiUrls.length === 2, 'OpenAI Responses: extracts 2 unique URLs'));
    results.push(assert(openaiUrls.includes('https://example.com/law1'), 'OpenAI Responses: contains law1'));
    results.push(assert(openaiUrls.includes('https://example.com/law2'), 'OpenAI Responses: contains law2'));

    // Test 2: OpenAI Responses API with no URLs
    const noUrls = extractToolUrlsFromResponse('openai', MOCK_OPENAI_RESPONSES_NO_URLS, true);
    results.push(assert(noUrls.length === 0, 'OpenAI Responses no URLs: returns empty'));

    // Test 3: Anthropic URL extraction
    const anthropicUrls = extractToolUrlsFromResponse('anthropic', MOCK_ANTHROPIC_RESPONSE, false);
    results.push(assert(anthropicUrls.length === 2, 'Anthropic: extracts 2 URLs'));
    results.push(assert(anthropicUrls[0].includes('anthropic-source'), 'Anthropic: correct domain'));

    // Test 4: Google URL extraction
    const googleUrls = extractToolUrlsFromResponse('google', MOCK_GOOGLE_RESPONSE, false);
    results.push(assert(googleUrls.length === 2, 'Google: extracts 2 grounding URLs'));

    // Test 5: Perplexity URL extraction
    const perplexityUrls = extractToolUrlsFromResponse('perplexity', MOCK_PERPLEXITY_RESPONSE, false);
    results.push(assert(perplexityUrls.length === 2, 'Perplexity: extracts 2 citation URLs'));

    // Test 6: isNoSearchToolError detection
    results.push(assert(isNoSearchToolError('openai', {}, 'I cannot perform web search', false), 'NoSearchError: detects "cannot perform web search"'));
    results.push(assert(isNoSearchToolError('openai', {}, 'no web search tool available', false), 'NoSearchError: detects "no web search tool"'));
    results.push(assert(!isNoSearchToolError('openai', {}, 'Here is the data you requested', false), 'NoSearchError: normal content is not error'));
    results.push(assert(isNoSearchToolError('openai', { status: 'failed' }, '', true), 'NoSearchError: Responses API failed status'));

    // Test 7: URL provenance enforcement
    const toolUrlSet = ['https://example.com/law1', 'https://example.com/law2'];
    results.push(assert(urlInList('https://example.com/law1', toolUrlSet.join(',')), 'Provenance: URL in tool_url_set passes'));
    results.push(assert(!urlInList('https://invented.com/fake', toolUrlSet.join(',')), 'Provenance: URL not in tool_url_set rejected'));
    results.push(assert(urlInList('https://example.com/law1/', toolUrlSet.join(',')), 'Provenance: trailing slash normalized'));

    // Test 8: Empty/null handling
    const emptyUrls = extractToolUrlsFromResponse('openai', null, true);
    results.push(assert(emptyUrls.length === 0, 'Null data: returns empty'));
    results.push(assert(!isNoSearchToolError('openai', null, null, false), 'Null everything: not an error'));

    // Test 9: OpenAI Chat Completions url_citation extraction
    const chatCitationUrls = extractToolUrlsFromResponse('openai', MOCK_OPENAI_CHAT_URLCITATIONS, false);
    results.push(assert(chatCitationUrls.length === 2, 'OpenAI Chat url_citation: extracts 2 URLs'));
    results.push(assert(chatCitationUrls.includes('https://chat-source.com/law1'), 'OpenAI Chat url_citation: contains law1'));

    // Test 10: Provenance enforcement — URL in tool set passes, fabricated URL rejected
    const provenanceToolUrls = ['https://real-source.com/law1', 'https://real-source.com/law2'];
    const realUrl = 'https://real-source.com/law1';
    const fakeUrl = 'https://fabricated.com/fake-law';
    const realNorm = realUrl.replace(/\/+$/, '').toLowerCase();
    const fakeNorm = fakeUrl.replace(/\/+$/, '').toLowerCase();
    results.push(assert(provenanceToolUrls.some(u => u.replace(/\/+$/, '').toLowerCase() === realNorm), 'Provenance: real URL passes tool set check'));
    results.push(assert(!provenanceToolUrls.some(u => u.replace(/\/+$/, '').toLowerCase() === fakeNorm), 'Provenance: fabricated URL rejected by tool set check'));

    // Test 11: Trailing slash normalization in tool URL provenance
    const trailingUrl = 'https://real-source.com/law1/';
    const trailingNorm = trailingUrl.replace(/\/+$/, '').toLowerCase();
    results.push(assert(provenanceToolUrls.some(u => u.replace(/\/+$/, '').toLowerCase() === trailingNorm), 'Provenance: trailing slash normalized in tool set'));

    // Test 12: Silent tool failure detection (no URLs returned = fail closed)
    const noToolUrlsData = { choices: [{ message: { content: 'Some analysis without URLs' }, finish_reason: 'stop' }] };
    const noToolUrls = extractToolUrlsFromResponse('openai', noToolUrlsData, false);
    results.push(assert(noToolUrls.length === 0, 'Silent failure: no tool URLs returns empty array'));

    // Test 13: SSRF safety — isSafeHttpUrl rejects private networks
    results.push(assert(!isSafeHttpUrl('http://localhost/test'), 'SSRF: rejects localhost'));
    results.push(assert(!isSafeHttpUrl('http://127.0.0.1/test'), 'SSRF: rejects 127.0.0.1'));
    results.push(assert(!isSafeHttpUrl('http://10.0.0.1/test'), 'SSRF: rejects 10.x'));
    results.push(assert(!isSafeHttpUrl('http://192.168.1.1/test'), 'SSRF: rejects 192.168.x'));
    results.push(assert(!isSafeHttpUrl('http://172.16.0.1/test'), 'SSRF: rejects 172.16.x'));
    results.push(assert(!isSafeHttpUrl('http://169.254.1.1/test'), 'SSRF: rejects 169.254.x'));
    results.push(assert(!isSafeHttpUrl('http://[::1]/test'), 'SSRF: rejects IPv6 loopback'));
    results.push(assert(!isSafeHttpUrl('ftp://example.com/file'), 'SSRF: rejects ftp'));
    results.push(assert(!isSafeHttpUrl('http://user:pass@example.com'), 'SSRF: rejects credentials'));
    results.push(assert(isSafeHttpUrl('https://www.legislation.gov.uk/test'), 'SSRF: allows legitimate URL'));

    // Test 14: Text content URLs are NOT extracted as tool URLs
    const textOnlyData = { choices: [{ message: { content: 'Found at https://example.com/law123 and https://example.com/law456', tool_calls: [] }, finish_reason: 'stop' }] };
    const textOnlyUrls = extractToolUrlsFromResponse('openai', textOnlyData, false);
    results.push(assert(textOnlyUrls.length === 0, 'Provenance: URLs in text content are not extracted as tool URLs'));

    return results;
}

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        if (!user) return Response.json({ error: 'Unauthorized' }, { status: 401 });

        const results = runTests();
        const allPassed = results.every(r => r.startsWith('PASS'));

        return Response.json({
            success: allPassed,
            total: results.length,
            passed: results.filter(r => r.startsWith('PASS')).length,
            failed: results.filter(r => r.startsWith('FAIL')).length,
            results,
        });
    } catch (error) {
        return Response.json({ error: error.message, stack: error.stack }, { status: 500 });
    }
});