import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

// ── PROVIDER REGISTRY ───────────────────────────────────────

const PROVIDERS = {
    openai: {
        label: 'OpenAI',
        icon: '🟢',
        modelsUrl:  (base) => `${base}/v1/models`,
        chatUrl:    (base, _model) => `${base}/v1/chat/completions`,
        authHeaders:(key) => ({ 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' }),
        parseModels:(data) => (data.data || []).map((m) => ({ id: m.id, name: m.id })),
        chatFormat: 'openai',
        cloudflareRisk: true,
        webSearchTool: 'web_search_preview',
        note: 'May be blocked by Cloudflare when called from cloud servers.',
    },
    openrouter: {
        label: 'Legacy (Removed)',
        icon: '🚫',
        modelsUrl:  (base) => `${base}/v1/models`,
        chatUrl:    (base) => `${base}/v1/chat/completions`,
        authHeaders:(key) => ({ 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' }),
        parseModels:(data) => (data.data || []).map((m) => ({ id: m.id, name: m.name || m.id })),
        chatFormat: 'openai',
        cloudflareRisk: false,
        webSearchTool: null,
        note: 'OpenRouter has been removed. Please use OpenAI direct or another supported provider.',
        removed: true,
    },
    anthropic: {
        label: 'Anthropic',
        icon: '🅰️',
        modelsUrl:  (base) => `${base}/v1/models`,
        chatUrl:    (base) => `${base}/v1/messages`,
        authHeaders:(key) => ({ 'x-api-key': key, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' }),
        parseModels:(data) => (data.data || []).map((m) => ({ id: m.id, name: m.display_name || m.id })),
        chatFormat: 'anthropic',
        cloudflareRisk: false,
        webSearchTool: 'web_search',
        note: '',
    },
    azure_openai: {
        label: 'Azure OpenAI',
        icon: '☁️',
        modelsUrl:  (base) => `${base}/openai/models?api-version=2024-10-21`,
        chatUrl:    (base, model) => `${base}/openai/deployments/${model}/chat/completions?api-version=2024-10-21`,
        authHeaders:(key) => ({ 'api-key': key, 'Content-Type': 'application/json' }),
        parseModels:(data) => (data.data || []).map((m) => ({ id: m.id, name: m.id })),
        chatFormat: 'openai',
        cloudflareRisk: false,
        webSearchTool: null,
        note: 'Use your Azure resource endpoint as the base URL.',
    },
    groq: {
        label: 'Groq',
        icon: '⚡',
        modelsUrl:  (base) => `${base}/openai/v1/models`,
        chatUrl:    (base) => `${base}/openai/v1/chat/completions`,
        authHeaders:(key) => ({ 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' }),
        parseModels:(data) => (data.data || []).map((m) => ({ id: m.id, name: m.id })),
        chatFormat: 'openai',
        cloudflareRisk: false,
        webSearchTool: null,
        note: 'Ultra-fast inference. Use https://api.groq.com as base URL.',
    },
    together: {
        label: 'Together AI',
        icon: '🤝',
        modelsUrl:  (base) => `${base}/v1/models`,
        chatUrl:    (base) => `${base}/v1/chat/completions`,
        authHeaders:(key) => ({ 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' }),
        parseModels:(data) => (data.data || []).map((m) => ({ id: m.id, name: m.display_name || m.id })),
        chatFormat: 'openai',
        cloudflareRisk: false,
        webSearchTool: null,
        note: '',
    },
    mistral: {
        label: 'Mistral AI',
        icon: '🌀',
        modelsUrl:  (base) => `${base}/v1/models`,
        chatUrl:    (base) => `${base}/v1/chat/completions`,
        authHeaders:(key) => ({ 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' }),
        parseModels:(data) => (data.data || []).map((m) => ({ id: m.id, name: m.id })),
        chatFormat: 'openai',
        cloudflareRisk: false,
        webSearchTool: null,
        note: '',
    },
    perplexity: {
        label: 'Perplexity',
        icon: '🔍',
        modelsUrl:  (_base) => null,
        chatUrl:    (base) => `${base}/chat/completions`,
        authHeaders:(key) => ({ 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' }),
        parseModels:(_data) => [],
        chatFormat: 'openai',
        cloudflareRisk: false,
        webSearchTool: 'builtin',
        note: 'All Perplexity models include built-in web search.',
    },
    google: {
        label: 'Google AI (Gemini)',
        icon: '🔷',
        modelsUrl:  (base) => `${base}/v1beta/models`,
        chatUrl:    (base, model) => `${base}/v1beta/models/${model}:generateContent`,
        authHeaders:(_key) => ({ 'Content-Type': 'application/json' }),
        parseModels:(data) => (data.models || [])
            .filter((m) => m.supportedGenerationMethods?.includes('generateContent'))
            .map((m) => ({ id: m.name?.replace('models/', ''), name: m.displayName || m.name })),
        chatFormat: 'google',
        cloudflareRisk: false,
        webSearchTool: 'google_search',
        note: 'Use https://generativelanguage.googleapis.com as base URL.',
    },
    openai_compatible: {
        label: 'OpenAI-Compatible',
        icon: '🔌',
        modelsUrl:  (base) => `${base}/v1/models`,
        chatUrl:    (base) => `${base}/v1/chat/completions`,
        authHeaders:(key) => ({ 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' }),
        parseModels:(data) => {
            const arr = data.data || data.models || [];
            return arr.map((m) => ({ id: m.id || m.name, name: m.name || m.id }));
        },
        chatFormat: 'openai',
        cloudflareRisk: false,
        webSearchTool: null,  // No server-side search — function-calling tools require client-side execution
        note: 'Generic fallback for any API that follows the OpenAI spec. Web search not available.',
    },
};

const PERPLEXITY_MODELS = [
    { id: 'sonar',              name: 'Sonar' },
    { id: 'sonar-pro',         name: 'Sonar Pro' },
    { id: 'sonar-reasoning',   name: 'Sonar Reasoning' },
    { id: 'sonar-reasoning-pro', name: 'Sonar Reasoning Pro' },
    { id: 'sonar-deep-research', name: 'Sonar Deep Research' },
    { id: 'r1-1776',           name: 'R1-1776' },
];

// ── PROVIDER AUTO-DETECTION ─────────────────────────────────

function detectProvider(baseUrl, apiKey) {
    const url = (baseUrl || '').toLowerCase().replace(/\/+$/, '');
    const key = (apiKey || '');

    if (url.includes('openrouter.ai'))                          return 'openrouter';
    if (url.includes('anthropic.com') || key.startsWith('sk-ant-')) return 'anthropic';
    if (url.includes('openai.azure.com'))                       return 'azure_openai';
    if (url.includes('api.groq.com') || url.includes('groq.com')) return 'groq';
    if (url.includes('together.xyz') || url.includes('together.ai')) return 'together';
    if (url.includes('mistral.ai'))                             return 'mistral';
    if (url.includes('perplexity.ai'))                          return 'perplexity';
    if (url.includes('generativelanguage.googleapis.com'))      return 'google';
    if (url.includes('api.openai.com'))                         return 'openai';

    return 'openai_compatible';
}

// ── CLOUDFLARE CHALLENGE DETECTION ──────────────────────────

function isCloudflareChallenge(status, body) {
    if (status !== 403 && status !== 503) return false;
    return body.includes('_cf_chl_opt') ||
           body.includes('challenge-platform') ||
           body.includes('cf-challenge') ||
           (body.includes('Cloudflare') && body.includes('Ray ID'));
}

// ── AES-256-GCM ENCRYPTION ─────────────────────────────────

function getEncryptionKey() {
    const key = Deno.env.get("ENCRYPTION_KEY");
    if (!key) throw new Error("ENCRYPTION_KEY environment variable is not set.");
    return key;
}

async function deriveKey(secret) {
    const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(secret));
    return crypto.subtle.importKey("raw", hash, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

async function encryptString(plaintext) {
    const key = await deriveKey(getEncryptionKey());
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const cipher = new Uint8Array(
        await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, new TextEncoder().encode(plaintext))
    );
    return `${btoa(String.fromCharCode(...iv))}.${btoa(String.fromCharCode(...cipher))}`;
}

async function decryptString(ciphertext) {
    if (!ciphertext.includes(".")) {
        try { return atob(ciphertext); }
        catch { throw new Error("Invalid legacy encrypted value"); }
    }
    const key = await deriveKey(getEncryptionKey());
    const [ivB64, cipherB64] = ciphertext.split(".");
    const iv          = Uint8Array.from(atob(ivB64),      c => c.charCodeAt(0));
    const cipherBytes = Uint8Array.from(atob(cipherB64),  c => c.charCodeAt(0));
    return new TextDecoder().decode(await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, cipherBytes));
}

async function decryptAndMigrate(conn, base44) {
    const plain = await decryptString(conn.api_key_encrypted);
    if (!conn.api_key_encrypted.includes(".")) {
        await base44.entities.APIConnection.update(conn.id, { api_key_encrypted: await encryptString(plain) });
    }
    return plain;
}

// ── FETCH HELPER WITH CLOUDFLARE DETECTION ──────────────────

async function safeFetch(url, options, providerKey) {
    const resp = await fetch(url, options);
    if (!resp.ok) {
        const body = await resp.text();
        if (isCloudflareChallenge(resp.status, body)) {
            const prov = PROVIDERS[providerKey];
            throw new Error(
                `CLOUDFLARE_BLOCKED: ${prov.label}'s servers returned a Cloudflare bot challenge (HTTP ${resp.status}). ` +
                `This happens when calling ${prov.label} from cloud infrastructure. ` +
                `Recommended fix: use OpenRouter (https://openrouter.ai/api) which proxies to ${prov.label} models without Cloudflare blocks.`
            );
        }
        throw new Error(`API returned ${resp.status}: ${body.slice(0, 500)}`);
    }
    return resp;
}

// ── GOOGLE HELPER (uses ?key= query param) ──────────────────
// Note: Google AI API requires key as query param. The Gemini API does not support
// x-goog-api-key header for generativelanguage.googleapis.com endpoints.
function googleFetchUrl(base, path, apiKey) {
    const url = new URL(path, base.replace(/\/+$/, '') + '/');
    url.searchParams.set('key', apiKey);
    return url.toString();
}

// ── WEB SEARCH AUTO-DETECTION ───────────────────────────────
// Tags models with web search support based on provider + model name + base URL.
// The third parameter baseUrl lets us identify openai_compatible providers.

function detectWebSearch(providerKey, modelId, baseUrl) {
    const id = (modelId || '').toLowerCase();
    const url = (baseUrl || '').toLowerCase();

    // Perplexity: all models have built-in web search
    if (providerKey === 'perplexity') {
        return { supports: true, options: ['builtin'] };
    }

    // Anthropic: Claude models support web_search tool
    if (providerKey === 'anthropic') {
        if (id.includes('claude')) {
            return { supports: true, options: ['web_search'] };
        }
        return { supports: false, options: [] };
    }

    // Google: Gemini models support google_search tool
    if (providerKey === 'google') {
        if (id.includes('gemini')) {
            return { supports: true, options: ['google_search'] };
        }
        return { supports: false, options: [] };
    }

    // OpenAI direct: strict allowlist for web search models
    // Only models verified to work with Responses API web_search tool
    const OPENAI_WEBSEARCH_ALLOWLIST = new Set([
        'gpt-4o', 'gpt-4o-mini', 'gpt-4o-search-preview',
        'gpt-4.1', 'gpt-4.1-mini', 'gpt-4.1-nano',
    ]);
    if (providerKey === 'openai') {
        // Check exact match first, then prefix match
        if (OPENAI_WEBSEARCH_ALLOWLIST.has(id)) {
            return { supports: true, options: ['web_search_preview'] };
        }
        for (const allowed of OPENAI_WEBSEARCH_ALLOWLIST) {
            if (id.startsWith(allowed + '-')) {
                return { supports: true, options: ['web_search_preview'] };
            }
        }
        return { supports: false, options: [] };
    }

    // OpenRouter: removed — always return no web search
    if (providerKey === 'openrouter') {
        return { supports: false, options: [] };
    }

    // ── OpenAI-Compatible: identify providers with real server-side search ──
    if (providerKey === 'openai_compatible') {

        // Moonshot / Kimi — uses server-side builtin_function $web_search.
        // The echo-loop protocol: client echoes tool arguments back, server
        // performs the actual web search behind the scenes.
        if (url.includes('moonshot') || url.includes('kimi')) {
            return { supports: true, options: ['kimi_web_search'] };
        }

        // DeepSeek, xAI, Cohere — these use client-side function-calling tools
        // that require the application to execute the search. Not supported
        // because we don't have a third-party search API integration.
        // Return false explicitly so the UI shows "Not supported".
        if (url.includes('deepseek') || url.includes('x.ai') || url.includes('xai.') || url.includes('cohere')) {
            return { supports: false, options: [] };
        }

        // Generic openai_compatible — cannot determine
        return { supports: null, options: [] };
    }

    // Groq, Together, Mistral, Azure — cannot reliably auto-detect
    return { supports: null, options: [] };
}

// ── STATIC MODEL PRICING ($ per million tokens) ────────────
const STATIC_PRICING = {
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
    'claude-sonnet-4':     { input: 3.00,  output: 15.00 },
    'claude-opus-4':       { input: 15.00, output: 75.00 },
    'claude-haiku-3.5':    { input: 0.80,  output: 4.00 },
    'claude-3-5-sonnet':   { input: 3.00,  output: 15.00 },
    'claude-3-5-haiku':    { input: 0.80,  output: 4.00 },
    'claude-3-opus':       { input: 15.00, output: 75.00 },
    'gemini-2.5-pro':      { input: 1.25,  output: 10.00 },
    'gemini-2.5-flash':    { input: 0.15,  output: 0.60 },
    'gemini-2.0-flash':    { input: 0.10,  output: 0.40 },
    'gemini-1.5-pro':      { input: 1.25,  output: 5.00 },
    'gemini-1.5-flash':    { input: 0.075, output: 0.30 },
    'moonshot-v1-auto':    { input: 0.55,  output: 0.55 },
    'moonshot-v1-8k':      { input: 0.17,  output: 0.17 },
    'moonshot-v1-32k':     { input: 0.33,  output: 0.33 },
    'moonshot-v1-128k':    { input: 0.83,  output: 0.83 },
    'kimi-latest':         { input: 0.55,  output: 0.55 },
    'deepseek-chat':       { input: 0.14,  output: 0.28 },
    'deepseek-reasoner':   { input: 0.55,  output: 2.19 },
    'sonar':               { input: 1.00,  output: 1.00 },
    'sonar-pro':           { input: 3.00,  output: 15.00 },
    'sonar-reasoning':     { input: 1.00,  output: 5.00 },
    'sonar-reasoning-pro': { input: 2.00,  output: 8.00 },
    'grok-3':              { input: 3.00,  output: 15.00 },
    'grok-3-mini':         { input: 0.30,  output: 0.50 },
    'grok-2':              { input: 2.00,  output: 10.00 },
    'mistral-large':       { input: 2.00,  output: 6.00 },
    'mistral-small':       { input: 0.10,  output: 0.30 },
};

function lookupStaticPricing(modelId) {
    const id = (modelId || '').toLowerCase();
    if (STATIC_PRICING[id]) return STATIC_PRICING[id];
    const sorted = Object.entries(STATIC_PRICING).sort(([a], [b]) => b.length - a.length);
    for (const [key, p] of sorted) {
        if (id.startsWith(key) || id.includes(key)) return p;
    }
    return null;
}

// OpenRouter pricing fetch removed — using static pricing table only

// ── MAIN HANDLER ────────────────────────────────────────────

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        if (!user) return Response.json({ error: 'Unauthorized' }, { status: 401 });

        const { action, ...params } = await req.json();

        switch (action) {

            case 'detectProvider': {
                const { base_url, api_key } = params;
                const providerKey = detectProvider(base_url || '', api_key || '');
                const prov = PROVIDERS[providerKey];
                return Response.json({
                    provider_type: providerKey,
                    label: prov.label,
                    icon: prov.icon,
                    note: prov.note,
                    cloudflareRisk: prov.cloudflareRisk,
                });
            }

            case 'testNew': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required to manage API connections' }, { status: 403 });
                }
                const { base_url, api_key } = params;
                if (!base_url || !api_key) {
                    return Response.json({ error: 'base_url and api_key are required' }, { status: 400 });
                }

                const cleanUrl = base_url.replace(/\/+$/, '');
                const providerKey = detectProvider(cleanUrl, api_key);
                
                if (providerKey === 'openrouter') {
                    return Response.json({ success: false, error: 'OpenRouter is no longer supported. Please use OpenAI direct or another provider.', provider_type: 'openrouter', label: 'Legacy (Removed)' });
                }
                const prov = PROVIDERS[providerKey];

                if (providerKey === 'perplexity') {
                    try {
                        const testResp = await safeFetch(
                            prov.chatUrl(cleanUrl),
                            {
                                method: 'POST',
                                headers: prov.authHeaders(api_key),
                                body: JSON.stringify({
                                    model: 'sonar',
                                    messages: [{ role: 'user', content: 'ping' }],
                                    max_tokens: 1,
                                }),
                            },
                            providerKey
                        );
                        await testResp.json();
                        return Response.json({
                            success: true,
                            provider_type: providerKey,
                            label: prov.label,
                            models: PERPLEXITY_MODELS,
                        });
                    } catch (e) {
                        return Response.json({ success: false, error: e.message, provider_type: providerKey, label: prov.label });
                    }
                }

                let modelsEndpoint = prov.modelsUrl(cleanUrl);
                let headers = prov.authHeaders(api_key);
                if (providerKey === 'google') {
                    modelsEndpoint = googleFetchUrl(cleanUrl, '/v1beta/models', api_key);
                }

                try {
                    const resp = await safeFetch(modelsEndpoint, { headers }, providerKey);
                    const data = await resp.json();
                    const models = prov.parseModels(data);
                    return Response.json({
                        success: true,
                        provider_type: providerKey,
                        label: prov.label,
                        models,
                    });
                } catch (e) {
                    return Response.json({
                        success: false,
                        error: e.message,
                        provider_type: providerKey,
                        label: prov.label,
                    });
                }
            }

            case 'create': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required to manage API connections' }, { status: 403 });
                }
                const { name, base_url, api_key } = params;
                if (!name || !base_url || !api_key) {
                    return Response.json({ error: 'name, base_url, and api_key are required' }, { status: 400 });
                }

                const cleanUrl = base_url.replace(/\/+$/, '');
                const providerKey = detectProvider(cleanUrl, api_key);
                
                if (providerKey === 'openrouter') {
                    return Response.json({ error: 'OpenRouter is no longer supported. Please use OpenAI direct or another provider.' }, { status: 400 });
                }
                const encrypted = await encryptString(api_key);

                const connection = await base44.entities.APIConnection.create({
                    name,
                    base_url: cleanUrl,
                    api_key_encrypted: encrypted,
                    provider_type: providerKey,
                    is_valid: true,
                });

                try {
                    await fetchAndStoreModels(base44, connection.id, cleanUrl, api_key, providerKey);
                } catch (_) { /* non-fatal */ }

                return Response.json({
                    success: true,
                    connection: { ...connection, api_key_encrypted: undefined },
                });
            }

            case 'testExisting': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required to manage API connections' }, { status: 403 });
                }
                const { connection_id } = params;
                const connections = await base44.entities.APIConnection.filter({ id: connection_id });
                if (!connections.length) return Response.json({ error: 'Not found' }, { status: 404 });

                const conn = connections[0];
                const apiKey = await decryptAndMigrate(conn, base44);
                const providerKey = conn.provider_type || detectProvider(conn.base_url, apiKey);
                const prov = PROVIDERS[providerKey];

                try {
                    if (providerKey === 'perplexity') {
                        await safeFetch(prov.chatUrl(conn.base_url), {
                            method: 'POST',
                            headers: prov.authHeaders(apiKey),
                            body: JSON.stringify({ model: 'sonar', messages: [{ role: 'user', content: 'ping' }], max_tokens: 1 }),
                        }, providerKey);
                    } else {
                        let url = prov.modelsUrl(conn.base_url);
                        if (providerKey === 'google') url = googleFetchUrl(conn.base_url, '/v1beta/models', apiKey);
                        await safeFetch(url, { headers: prov.authHeaders(apiKey) }, providerKey);
                    }

                    await base44.entities.APIConnection.update(connection_id, {
                        is_valid: true,
                        provider_type: providerKey,
                        last_tested_at: new Date().toISOString(),
                    });
                    return Response.json({ success: true, provider_type: providerKey });
                } catch (e) {
                    await base44.entities.APIConnection.update(connection_id, { is_valid: false });
                    return Response.json({ success: false, error: e.message });
                }
            }

            case 'list': {
                const connections = await base44.entities.APIConnection.filter({ created_by: user.email });
                const enriched = [];
                for (const c of connections) {
                    const models = await base44.entities.ModelCatalog.filter({ connection_id: c.id });
                    enriched.push({
                        ...c,
                        api_key_encrypted: undefined,
                        has_key: !!c.api_key_encrypted,
                        model_count: models.length,
                        web_search_model_count: models.filter(m => m.supports_web_search === true).length,
                    });
                }
                return Response.json({ connections: enriched });
            }

            case 'delete': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required to manage API connections' }, { status: 403 });
                }
                const { connection_id } = params;
                const models = await base44.entities.ModelCatalog.filter({ connection_id });
                
                // Delete models in small batches with delays to avoid rate limits
                for (let i = 0; i < models.length; i++) {
                    for (let attempt = 0; attempt < 3; attempt++) {
                        try {
                            await base44.entities.ModelCatalog.delete(models[i].id);
                            break;
                        } catch (e) {
                            if (e.message?.includes('429') || e.message?.includes('Rate limit')) {
                                await new Promise(r => setTimeout(r, 2000 * (attempt + 1)));
                            } else {
                                throw e;
                            }
                        }
                    }
                    // Throttle: pause every 5 deletes
                    if ((i + 1) % 5 === 0) await new Promise(r => setTimeout(r, 1000));
                }
                
                await base44.entities.APIConnection.delete(connection_id);
                return Response.json({ success: true });
            }

            case 'fetchModels': {
                const { connection_id } = params;
                const connections = await base44.entities.APIConnection.filter({ id: connection_id });
                if (!connections.length) return Response.json({ error: 'Not found' }, { status: 404 });

                const conn = connections[0];
                const apiKey = await decryptAndMigrate(conn, base44);
                const providerKey = conn.provider_type || detectProvider(conn.base_url, apiKey);

                const models = await fetchAndStoreModels(base44, connection_id, conn.base_url, apiKey, providerKey);
                return Response.json({ models, provider_type: providerKey });
            }

            case 'probeWebSearch': {
                const { connection_id, model_id } = params;
                const connections = await base44.entities.APIConnection.filter({ id: connection_id });
                if (!connections.length) return Response.json({ error: 'Not found' }, { status: 404 });

                const conn = connections[0];
                const apiKey = await decryptAndMigrate(conn, base44);
                const providerKey = conn.provider_type || detectProvider(conn.base_url, apiKey);
                const prov = PROVIDERS[providerKey];

                let supportsWebSearch = false;
                let webSearchOptions = [];

                if (providerKey === 'perplexity') {
                    supportsWebSearch = true;
                    webSearchOptions = ['builtin'];
                } else if (providerKey === 'anthropic') {
                    try {
                        const r = await fetch(prov.chatUrl(conn.base_url), {
                            method: 'POST',
                            headers: prov.authHeaders(apiKey),
                            body: JSON.stringify({
                                model: model_id,
                                messages: [{ role: 'user', content: 'What is 1+1?' }],
                                max_tokens: 5,
                                tools: [{ type: 'web_search_20250305', name: 'web_search' }],
                            }),
                        });
                        if (r.ok || r.status === 400) {
                            const body = await r.json();
                            if (!body.error?.message?.includes('not support')) {
                                supportsWebSearch = true;
                                webSearchOptions = ['web_search'];
                            }
                        }
                    } catch (_) {}
                } else if (prov.chatFormat === 'openai') {
                    try {
                        const r = await safeFetch(prov.chatUrl(conn.base_url, model_id), {
                            method: 'POST',
                            headers: prov.authHeaders(apiKey),
                            body: JSON.stringify({
                                model: model_id,
                                messages: [{ role: 'user', content: 'What is 1+1?' }],
                                max_tokens: 5,
                                tools: [{ type: 'function', function: { name: 'web_search', description: 'Search the web', parameters: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] } } }],
                            }),
                        }, providerKey);
                        if (r.ok) {
                            supportsWebSearch = true;
                            webSearchOptions = ['web_search'];
                        }
                    } catch (_) {}
                }

                const existing = await base44.entities.ModelCatalog.filter({ connection_id, model_id });
                if (existing.length > 0) {
                    await base44.entities.ModelCatalog.update(existing[0].id, {
                        supports_web_search: supportsWebSearch,
                        web_search_options: webSearchOptions,
                        last_checked_at: new Date().toISOString(),
                    });
                }

                return Response.json({ supports_web_search: supportsWebSearch, web_search_options: webSearchOptions });
            }

            case 'getModels': {
                const { connection_id } = params;
                let models = await base44.entities.ModelCatalog.filter({ connection_id });
                const connections = await base44.entities.APIConnection.filter({ id: connection_id });
                const conn = connections[0];

                // If no cached models exist, try a live fetch as fallback
                if (models.length === 0 && conn) {
                    try {
                        const apiKey = await decryptAndMigrate(conn, base44);
                        const pk = conn.provider_type || detectProvider(conn.base_url, apiKey);
                        models = await fetchAndStoreModels(base44, conn.id, conn.base_url, apiKey, pk);
                    } catch (_) {
                        // Live fetch failed — return empty, user can manually refresh
                    }
                }

                // Re-apply web search detection on ALL cached models.
                // This patches models cached before detectWebSearch was expanded,
                // including ones previously marked false that may now be detected as true.
                if (conn && models.length > 0) {
                    const pk = conn.provider_type || 'openai_compatible';
                    for (const m of models) {
                        const ws = detectWebSearch(pk, m.model_id, conn.base_url);
                        if (ws.supports !== null && (
                            m.supports_web_search !== ws.supports ||
                            JSON.stringify(m.web_search_options || []) !== JSON.stringify(ws.options)
                        )) {
                            try {
                                await base44.entities.ModelCatalog.update(m.id, {
                                    supports_web_search: ws.supports,
                                    web_search_options: ws.options,
                                    last_checked_at: new Date().toISOString(),
                                });
                                m.supports_web_search = ws.supports;
                                m.web_search_options = ws.options;
                            } catch (_) {}
                        }
                    }
                }

                return Response.json({
                    models,
                    provider_type: conn?.provider_type || null,
                });
            }

            case 'refreshAllModels': {
                const allConnections = await base44.entities.APIConnection.list();
                const results = [];
                for (const conn of allConnections) {
                    try {
                        const apiKey = await decryptString(conn.api_key_encrypted);
                        const pk = conn.provider_type || detectProvider(conn.base_url, apiKey);
                        const models = await fetchAndStoreModels(base44, conn.id, conn.base_url, apiKey, pk);
                        results.push({ id: conn.id, name: conn.name, success: true, model_count: models.length });
                    } catch (e) {
                        results.push({ id: conn.id, name: conn.name, success: false, error: e.message });
                    }
                }
                return Response.json({ results });
            }

            case 'getProviders': {
                const summary = Object.entries(PROVIDERS).map(([key, p]) => ({
                    key,
                    label: p.label,
                    icon: p.icon,
                    note: p.note,
                    cloudflareRisk: p.cloudflareRisk,
                }));
                return Response.json({ providers: summary });
            }

            case 'fetchPricing': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin only' }, { status: 403 });
                }
                const { connection_id: pricingConnId } = params;

                let pricingModels;
                if (pricingConnId) {
                    pricingModels = await base44.entities.ModelCatalog.filter({ connection_id: pricingConnId });
                } else {
                    pricingModels = await base44.entities.ModelCatalog.list();
                }

                let pricingUpdated = 0;
                for (const m of pricingModels) {
                    const mid = (m.model_id || '').toLowerCase();
                    if (m.pricing_source === 'manual') continue;
                    const price = lookupStaticPricing(mid);
                    if (price && (m.input_price_per_million !== price.input || m.output_price_per_million !== price.output)) {
                        await base44.entities.ModelCatalog.update(m.id, {
                            input_price_per_million: price.input,
                            output_price_per_million: price.output,
                            pricing_source: 'static',
                        });
                        pricingUpdated++;
                    }
                }
                return Response.json({ success: true, updated: pricingUpdated, total: pricingModels.length });
            }

            case 'updateModelPrice': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin only' }, { status: 403 });
                }
                const { model_catalog_id, input_price, output_price } = params;
                if (!model_catalog_id) return Response.json({ error: 'model_catalog_id required' }, { status: 400 });
                await base44.entities.ModelCatalog.update(model_catalog_id, {
                    input_price_per_million: parseFloat(input_price) || 0,
                    output_price_per_million: parseFloat(output_price) || 0,
                    pricing_source: 'manual',
                });
                return Response.json({ success: true });
            }

            case 'getModelPricing': {
                const allModels = await base44.entities.ModelCatalog.list();
                const allConns = await base44.entities.APIConnection.list();
                const connMap = {};
                allConns.forEach(c => { connMap[c.id] = c.name; });
                const result = allModels.map(m => ({
                    id: m.id, model_id: m.model_id, display_name: m.display_name,
                    connection_id: m.connection_id, connection_name: connMap[m.connection_id] || 'Unknown',
                    input_price_per_million: m.input_price_per_million || 0,
                    output_price_per_million: m.output_price_per_million || 0,
                    pricing_source: m.pricing_source || '',
                }));
                return Response.json({ models: result });
            }

            default:
                return Response.json({ error: `Unknown action: ${action}` }, { status: 400 });
        }
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});

// ── SHARED: Fetch models and store in ModelCatalog ──────────

async function fetchAndStoreModels(base44, connectionId, baseUrl, apiKey, providerKey) {
    const prov = PROVIDERS[providerKey];
    let models = [];

    if (providerKey === 'perplexity') {
        models = PERPLEXITY_MODELS;
    } else if (providerKey === 'google') {
        const url = googleFetchUrl(baseUrl, '/v1beta/models', apiKey);
        const resp = await safeFetch(url, { headers: { 'Content-Type': 'application/json' } }, providerKey);
        models = prov.parseModels(await resp.json());
    } else {
        const endpoint = prov.modelsUrl(baseUrl);
        if (!endpoint) return [];
        const resp = await safeFetch(endpoint, { headers: prov.authHeaders(apiKey) }, providerKey);
        models = prov.parseModels(await resp.json());
    }

    const now = new Date().toISOString();

    for (const m of models) {
        // Auto-detect web search support from provider + model name
        const ws = detectWebSearch(providerKey, m.id, baseUrl);

        const existing = await base44.entities.ModelCatalog.filter({
            connection_id: connectionId,
            model_id: m.id,
        });

        const pricing = lookupStaticPricing(m.id);

        if (existing.length === 0) {
            await base44.entities.ModelCatalog.create({
                connection_id: connectionId,
                model_id: m.id,
                display_name: m.name,
                supports_web_search: ws.supports,
                web_search_options: ws.options,
                last_checked_at: now,
                input_price_per_million: pricing?.input || 0,
                output_price_per_million: pricing?.output || 0,
                pricing_source: pricing ? 'static' : '',
            });
        } else {
            const update = {
                display_name: m.name,
                last_checked_at: now,
            };
            if (ws.supports !== null) {
                update.supports_web_search = ws.supports;
                update.web_search_options = ws.options;
            }
            if ((!existing[0].input_price_per_million || existing[0].pricing_source !== 'manual') && pricing) {
                update.input_price_per_million = pricing.input;
                update.output_price_per_million = pricing.output;
                update.pricing_source = 'static';
            }
            await base44.entities.ModelCatalog.update(existing[0].id, update);
        }
    }

    return await base44.entities.ModelCatalog.filter({ connection_id: connectionId });
}