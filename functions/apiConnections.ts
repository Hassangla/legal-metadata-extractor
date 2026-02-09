import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

// ── AES-256-GCM crypto helpers ──────────────────────────────────

function getEncryptionKey() {
    const key = Deno.env.get("ENCRYPTION_KEY");
    if (!key) {
        throw new Error("ENCRYPTION_KEY environment variable is not set. Cannot encrypt/decrypt API keys.");
    }
    return key;
}

async function deriveKey(secret) {
    const enc = new TextEncoder();
    const hash = await crypto.subtle.digest("SHA-256", enc.encode(secret));
    return crypto.subtle.importKey("raw", hash, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

async function encryptString(plaintext) {
    const secret = getEncryptionKey();
    const key = await deriveKey(secret);
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const enc = new TextEncoder();
    const cipherBytes = new Uint8Array(
        await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, enc.encode(plaintext))
    );
    const ivB64 = btoa(String.fromCharCode(...iv));
    const cipherB64 = btoa(String.fromCharCode(...cipherBytes));
    return `${ivB64}.${cipherB64}`;
}

async function decryptString(ciphertext) {
    // Legacy fallback: if no "." separator, treat as old base64-only value
    if (!ciphertext.includes(".")) {
        try {
            return atob(ciphertext);
        } catch {
            throw new Error("Failed to decrypt API key (invalid legacy format)");
        }
    }

    const secret = getEncryptionKey();
    const key = await deriveKey(secret);
    const [ivB64, cipherB64] = ciphertext.split(".");
    const iv = Uint8Array.from(atob(ivB64), c => c.charCodeAt(0));
    const cipherBytes = Uint8Array.from(atob(cipherB64), c => c.charCodeAt(0));
    const plainBuf = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, cipherBytes);
    return new TextDecoder().decode(plainBuf);
}

/**
 * Decrypt and auto-migrate legacy keys to AES-GCM format.
 * Returns the plaintext API key.
 */
async function decryptAndMigrate(conn, base44) {
    const plaintext = await decryptString(conn.api_key_encrypted);

    // If it was legacy (no dot), re-encrypt and persist
    if (!conn.api_key_encrypted.includes(".")) {
        const encrypted = await encryptString(plaintext);
        await base44.entities.APIConnection.update(conn.id, { api_key_encrypted: encrypted });
    }

    return plaintext;
}

// ── Main handler ────────────────────────────────────────────────

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const { action, ...params } = await req.json();

        switch (action) {
            case 'create': {
                const { name, base_url, api_key } = params;
                
                const encrypted = await encryptString(api_key);
                
                const connection = await base44.entities.APIConnection.create({
                    name,
                    base_url: base_url.replace(/\/$/, ''),
                    api_key_encrypted: encrypted,
                    is_valid: false
                });
                
                return Response.json({ 
                    success: true, 
                    connection: { ...connection, api_key_encrypted: undefined } 
                });
            }
            
            case 'testNew': {
                const { base_url, api_key } = params;
                
                if (!base_url || !api_key) {
                    return Response.json({ error: 'base_url and api_key are required' }, { status: 400 });
                }
                
                const cleanUrl = base_url.replace(/\/$/, '');
                const response = await fetch(`${cleanUrl}/v1/models`, {
                    headers: {
                        'Authorization': `Bearer ${api_key}`,
                        'Content-Type': 'application/json'
                    }
                });
                
                if (!response.ok) {
                    const errorText = await response.text();
                    return Response.json({ 
                        success: false, 
                        error: `API returned ${response.status}: ${errorText}` 
                    });
                }
                
                const data = await response.json();
                return Response.json({ 
                    success: true, 
                    models: data.data || data.models || [] 
                });
            }
            
            case 'testExisting': {
                const { connection_id } = params;
                
                if (!connection_id) {
                    return Response.json({ error: 'connection_id is required' }, { status: 400 });
                }
                
                const connections = await base44.entities.APIConnection.filter({ id: connection_id });
                if (!connections.length) {
                    return Response.json({ error: 'Connection not found' }, { status: 404 });
                }
                
                const conn = connections[0];
                const apiKey = await decryptAndMigrate(conn, base44);
                
                const response = await fetch(`${conn.base_url}/v1/models`, {
                    headers: {
                        'Authorization': `Bearer ${apiKey}`,
                        'Content-Type': 'application/json'
                    }
                });
                
                if (!response.ok) {
                    const errorText = await response.text();
                    return Response.json({ 
                        success: false, 
                        error: `API returned ${response.status}: ${errorText}` 
                    });
                }
                
                const data = await response.json();
                
                await base44.entities.APIConnection.update(connection_id, {
                    is_valid: true,
                    last_tested_at: new Date().toISOString()
                });
                
                return Response.json({ 
                    success: true, 
                    models: data.data || data.models || [] 
                });
            }
            
            case 'list': {
                const connections = await base44.entities.APIConnection.filter({ created_by: user.email });
                const safeConnections = connections.map(c => ({
                    ...c,
                    api_key_encrypted: undefined,
                    has_key: !!c.api_key_encrypted
                }));
                return Response.json({ connections: safeConnections });
            }
            
            case 'delete': {
                const { connection_id } = params;
                await base44.entities.APIConnection.delete(connection_id);
                return Response.json({ success: true });
            }
            
            case 'fetchModels': {
                const { connection_id } = params;
                
                const connections = await base44.entities.APIConnection.filter({ id: connection_id });
                if (!connections.length) {
                    return Response.json({ error: 'Connection not found' }, { status: 404 });
                }
                
                const conn = connections[0];
                const apiKey = await decryptAndMigrate(conn, base44);
                
                const response = await fetch(`${conn.base_url}/v1/models`, {
                    headers: {
                        'Authorization': `Bearer ${apiKey}`,
                        'Content-Type': 'application/json'
                    }
                });
                
                if (!response.ok) {
                    return Response.json({ error: 'Failed to fetch models' }, { status: 500 });
                }
                
                const data = await response.json();
                const models = data.data || data.models || [];
                
                for (const model of models) {
                    const modelId = model.id || model.name;
                    const existing = await base44.entities.ModelCatalog.filter({ 
                        connection_id, 
                        model_id: modelId 
                    });
                    
                    if (existing.length === 0) {
                        await base44.entities.ModelCatalog.create({
                            connection_id,
                            model_id: modelId,
                            display_name: model.name || model.id,
                            supports_web_search: null,
                            web_search_options: [],
                            last_checked_at: null
                        });
                    }
                }
                
                const catalog = await base44.entities.ModelCatalog.filter({ connection_id });
                
                return Response.json({ models: catalog });
            }
            
            case 'probeWebSearch': {
                const { connection_id, model_id } = params;
                
                const connections = await base44.entities.APIConnection.filter({ id: connection_id });
                if (!connections.length) {
                    return Response.json({ error: 'Connection not found' }, { status: 404 });
                }
                
                const conn = connections[0];
                const apiKey = await decryptAndMigrate(conn, base44);
                
                let supportsWebSearch = false;
                let webSearchOptions = [];
                
                try {
                    const testResponse = await fetch(`${conn.base_url}/v1/chat/completions`, {
                        method: 'POST',
                        headers: {
                            'Authorization': `Bearer ${apiKey}`,
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            model: model_id,
                            messages: [{ role: 'user', content: 'Test' }],
                            max_tokens: 1,
                            tools: [{ type: 'web_search' }]
                        })
                    });
                    
                    if (testResponse.ok) {
                        supportsWebSearch = true;
                        webSearchOptions.push('web_search');
                    }
                } catch (e) {
                    // Tool not supported in this format
                }
                
                if (conn.base_url.includes('openrouter')) {
                    supportsWebSearch = true;
                    webSearchOptions = ['openrouter_web_search'];
                }
                
                if (conn.base_url.includes('perplexity') || model_id.includes('sonar')) {
                    supportsWebSearch = true;
                    webSearchOptions = ['perplexity_online'];
                }
                
                const existing = await base44.entities.ModelCatalog.filter({ 
                    connection_id, 
                    model_id 
                });
                
                if (existing.length > 0) {
                    await base44.entities.ModelCatalog.update(existing[0].id, {
                        supports_web_search: supportsWebSearch,
                        web_search_options: webSearchOptions,
                        last_checked_at: new Date().toISOString()
                    });
                }
                
                return Response.json({ 
                    supports_web_search: supportsWebSearch,
                    web_search_options: webSearchOptions
                });
            }
            
            default:
                return Response.json({ error: 'Unknown action' }, { status: 400 });
        }
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});