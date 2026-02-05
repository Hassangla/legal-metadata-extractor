import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

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
                
                // Simple XOR encryption for API key (in production, use proper encryption)
                const encryptionKey = Deno.env.get("ENCRYPTION_KEY") || "default-key-change-me";
                const encrypted = btoa(api_key); // Base64 encode for now
                
                const connection = await base44.entities.APIConnection.create({
                    name,
                    base_url: base_url.replace(/\/$/, ''), // Remove trailing slash
                    api_key_encrypted: encrypted,
                    is_valid: false
                });
                
                return Response.json({ 
                    success: true, 
                    connection: { ...connection, api_key_encrypted: undefined } 
                });
            }
            
            case 'test': {
                const { connection_id, api_key } = params;
                
                let apiKey = api_key;
                let baseUrl;
                
                if (connection_id) {
                    const connections = await base44.entities.APIConnection.filter({ id: connection_id });
                    if (!connections.length) {
                        return Response.json({ error: 'Connection not found' }, { status: 404 });
                    }
                    const conn = connections[0];
                    baseUrl = conn.base_url;
                    if (!apiKey) {
                        apiKey = atob(conn.api_key_encrypted);
                    }
                } else {
                    baseUrl = params.base_url?.replace(/\/$/, '');
                }
                
                // Test connection by fetching models
                const response = await fetch(`${baseUrl}/v1/models`, {
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
                
                if (connection_id) {
                    await base44.entities.APIConnection.update(connection_id, {
                        is_valid: true,
                        last_tested_at: new Date().toISOString()
                    });
                }
                
                return Response.json({ 
                    success: true, 
                    models: data.data || data.models || [] 
                });
            }
            
            case 'list': {
                const connections = await base44.entities.APIConnection.filter({ created_by: user.email });
                // Remove encrypted keys from response
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
                const apiKey = atob(conn.api_key_encrypted);
                
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
                
                // Store/update models in catalog
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
                            supports_web_search: null, // Unknown until probed
                            web_search_options: [],
                            last_checked_at: null
                        });
                    }
                }
                
                // Fetch updated catalog
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
                const apiKey = atob(conn.api_key_encrypted);
                
                // Try to probe web search capability
                let supportsWebSearch = false;
                let webSearchOptions = [];
                
                // Try OpenAI-style tool calling with web search
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
                
                // Check for OpenRouter-style web search
                if (conn.base_url.includes('openrouter')) {
                    supportsWebSearch = true;
                    webSearchOptions = ['openrouter_web_search'];
                }
                
                // Check for Perplexity-style
                if (conn.base_url.includes('perplexity') || model_id.includes('sonar')) {
                    supportsWebSearch = true;
                    webSearchOptions = ['perplexity_online'];
                }
                
                // Update model catalog
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