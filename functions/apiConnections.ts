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
        note: 'May be blocked by Cloudflare when called from cloud servers. Consider using OpenRouter instead.',
    },
    openrouter: {
        label: 'OpenRouter',
        icon: '🔀',
        modelsUrl:  (base) => `${base}/v1/models`,
        chatUrl:    (base) => `${base}/v1/chat/completions`,
        authHeaders:(key) => ({ 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' }),
        parseModels:(data) => (data.data || []).map((m) => ({ id: m.id, name: m.name || m.id })),
        chatFormat: 'openai',
        cloudflareRisk: false,
        webSearchTool: null,
        note: 'Proxies to OpenAI, Anthropic, Google, and many other models. Recommended for cloud use.',
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
        webSearchTool: null,
        note: 'Generic fallback for any API that follows the OpenAI spec.',
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

    // OpenAI direct: GPT-4o/4.1/4.5, o-series, chatgpt-4o support web search
    if (providerKey === 'openai') {
        if (id.includes('gpt-4o') || id.includes('gpt-4.1') || id.includes('gpt-4.5') ||
            id.includes('gpt-4-turbo') ||
            id.startsWith('o1') || id.startsWith('o3') || id.startsWith('o4') ||
            id.startsWith('chatgpt-4o')) {
            return { supports: true, options: ['web_search_preview'] };
        }
        return { supports: false, options: [] };
    }

    // OpenRouter: detect by model path prefix
    if (providerKey === 'openrouter') {
        if (id.includes('anthropic/claude')) {
            return { supports: true, options: ['web_search'] };
        }
        if (id.includes('openai/gpt-4o') || id.includes('openai/gpt-4.1') ||
            id.includes('openai/gpt-4.5') || id.includes('openai/chatgpt-4o') ||
            id.match(/openai\/o[134]/)) {
            return { supports: true, options: ['web_search_preview'] };
        }
        if (id.includes('google/gemini')) {
            return { supports: true, options: ['google_search'] };
        }
        if (id.includes('perplexity/')) {
            return { supports: true, options: ['builtin'] };
        }
        return { supports: null, options: [] };
    }

    // ── OpenAI-Compatible: identify known providers by base URL ──
    if (providerKey === 'openai_compatible') {

        // Moonshot / Kimi — supports function calling with web search
        if (url.includes('moonshot') || url.includes('kimi')) {
            return { supports: true, options: ['web_search'] };
        }

        // DeepSeek — deepseek-chat and deepseek-v* support tool use
        if (url.includes('deepseek')) {
            if (id.includes('deepseek-chat') || id.includes('deepseek-v') || id.includes('deepseek-r')) {
                return { supports: true, options: ['web_search'] };
            }
            return { supports: null, options: [] };
        }

        // xAI / Grok — grok models support function calling
        if (url.includes('x.ai') || url.includes('xai.')) {
            if (id.includes('grok')) {
                return { supports: true, options: ['web_search'] };
            }
            return { supports: null, options: [] };
        }

        // Cohere — command models support web connectors
        if (url.includes('cohere')) {
            if (id.includes('command')) {
                return { supports: true, options: ['web_search'] };
            }
            return { supports: false, options: [] };
        }

        // Generic openai_compatible — cannot determine
        return { supports: null, options: [] };
    }

    // Groq, Together, Mistral, Azure — cannot reliably auto-detect
    return { supports: null, options: [] };
}

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
                const { base_url, api_key } = params;
                if (!base_url || !api_key) {
                    return Response.json({ error: 'base_url and api_key are required' }, { status: 400 });
                }

                const cleanUrl = base_url.replace(/\/+$/, '');
                const providerKey = detectProvider(cleanUrl, api_key);
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
                const { name, base_url, api_key } = params;
                if (!name || !base_url || !api_key) {
                    return Response.json({ error: 'name, base_url, and api_key are required' }, { status: 400 });
                }

                const cleanUrl = base_url.replace(/\/+$/, '');
                const providerKey = detectProvider(cleanUrl, api_key);
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
                const { connection_id } = params;
                const models = await base44.entities.ModelCatalog.filter({ connection_id });
                for (const m of models) await base44.entities.ModelCatalog.delete(m.id);
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

                // Re-apply web search detection on cached models that still have null.
                // This patches models cached before detectWebSearch was expanded.
                if (conn && models.length > 0) {
                    const pk = conn.provider_type || 'openai_compatible';
                    for (const m of models) {
                        if (m.supports_web_search === null || m.supports_web_search === undefined) {
                            const ws = detectWebSearch(pk, m.model_id, conn.base_url);
                            if (ws.supports !== null) {
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

        if (existing.length === 0) {
            await base44.entities.ModelCatalog.create({
                connection_id: connectionId,
                model_id: m.id,
                display_name: m.name,
                supports_web_search: ws.supports,
                web_search_options: ws.options,
                last_checked_at: now,
            });
        } else {
            // Preserve manually probed web search if auto-detect returns null
            const update = {
                display_name: m.name,
                last_checked_at: now,
            };
            if (ws.supports !== null) {
                update.supports_web_search = ws.supports;
                update.web_search_options = ws.options;
            }
            await base44.entities.ModelCatalog.update(existing[0].id, update);
        }
    }

    return await base44.entities.ModelCatalog.filter({ connection_id: connectionId });
}