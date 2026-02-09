import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

const BATCH_SIZE = 5;
const MAX_RETRIES = 3;
const RETRY_BASE_MS = 2000;

// ── PROVIDER CHAT CONFIGS ───────────────────────────────────

const CHAT_CONFIGS = {
    openai:           { chatUrl: (b) => `${b}/v1/chat/completions`,          authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    openrouter:       { chatUrl: (b) => `${b}/v1/chat/completions`,          authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    groq:             { chatUrl: (b) => `${b}/openai/v1/chat/completions`,   authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    together:         { chatUrl: (b) => `${b}/v1/chat/completions`,          authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    mistral:          { chatUrl: (b) => `${b}/v1/chat/completions`,          authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    perplexity:       { chatUrl: (b) => `${b}/chat/completions`,             authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    openai_compatible:{ chatUrl: (b) => `${b}/v1/chat/completions`,          authHeaders: (k) => ({ 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    azure_openai:     { chatUrl: (b, m) => `${b}/openai/deployments/${m}/chat/completions?api-version=2024-10-21`, authHeaders: (k) => ({ 'api-key': k, 'Content-Type': 'application/json' }), chatFormat: 'openai' },
    anthropic:        { chatUrl: (b) => `${b}/v1/messages`,                  authHeaders: (k) => ({ 'x-api-key': k, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' }), chatFormat: 'anthropic' },
    google:           { chatUrl: (b, m) => `${b}/v1beta/models/${m}:generateContent`, authHeaders: (_) => ({ 'Content-Type': 'application/json' }), chatFormat: 'google' },
};

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
    const body = {
        model: modelId,
        messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: userPrompt },
        ],
        max_tokens: 4096,
        temperature: 0,
        response_format: { type: 'json_object' },
    };
    if (webSearchChoice && webSearchChoice !== 'none' && webSearchChoice !== 'builtin') {
        body.tools = [{ type: 'function', function: { name: 'web_search', description: 'Search the web', parameters: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] } } }];
    }
    return {
        url: cfg.chatUrl(baseUrl, modelId),
        init: { method: 'POST', headers: cfg.authHeaders(apiKey), body: JSON.stringify(body) },
    };
}

// ── PARSE LLM RESPONSE ─────────────────────────────────────

function extractTextContent(providerType, data) {
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
    return data.choices?.[0]?.message?.content || '';
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

// ── MAIN HANDLER ────────────────────────────────────────────

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        if (!user) return Response.json({ error: 'Unauthorized' }, { status: 401 });

        const { action, ...params } = await req.json();

        switch (action) {

            case 'create': {
                const { connection_id, model_id, web_search_choice, input_file_url, input_file_name, total_rows, input_rows } = params;

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

                const allRows = await base44.entities.JobRow.filter({ job_id });
                const pendingRows = allRows
                    .filter((r) => r.status === 'pending')
                    .sort((a, b) => a.row_index - b.row_index)
                    .slice(0, BATCH_SIZE);

                if (!pendingRows.length) {
                    await base44.entities.Job.update(job_id, { status: 'done', processed_rows: job.total_rows });
                    return Response.json({ job: { ...job, status: 'done' }, message: 'All rows processed' });
                }

                let processedCount = 0;

                for (const row of pendingRows) {
                    try {
                        await base44.entities.JobRow.update(row.id, { status: 'processing' });
                        const input = row.input_data;
                        const economyCode = economyMap[input.Economy?.toLowerCase()?.trim()] || '';

                        const query1 = `${input.Legal_basis || input['Legal basis']} ${input.Economy} official text`;
                        const query2 = `${input.Legal_basis || input['Legal basis']} ${input.Economy} legislation database`;
                        const query3 = `${input.Topic} ${input.Economy} legal instrument ${input.Question}`;

                        const systemPrompt = `You are a legal metadata extraction assistant. Follow the specification below EXACTLY. Always respond with valid JSON only — no markdown, no explanation.\n\n${specText}`;

                        const userPrompt = `Extract metadata for this row:
- Owner: ${input.Owner}
- Economy: ${input.Economy}
- Economy_Code: ${economyCode}
- Legal basis: ${input.Legal_basis || input['Legal basis']}
- Question: ${input.Question}
- Topic: ${input.Topic}

Search queries to use:
1. ${query1}
2. ${query2}
3. ${query3}

Return a JSON object with exactly this structure:
{
  "output": {
    "Owner": "${input.Owner}",
    "Economy": "${input.Economy}",
    "Economy_Code": "${economyCode}",
    "Legal_basis": "${input.Legal_basis || input['Legal basis']}",
    "Question": "${input.Question}",
    "Topic": "${input.Topic}",
    "Instrument_Title": "extracted title",
    "Instrument_URL": "source URL",
    "Instrument_Date": "YYYY-MM-DD",
    "Instrument_Type": "type",
    "Extraction_Status": "success|partial|failed",
    "Confidence_Score": 0.0,
    "Processing_Notes": ""
  },
  "evidence": {
    "Row_Index": ${row.row_index},
    "Query_1": "${query1}",
    "Query_2": "${query2}",
    "Query_3": "${query3}",
    "URLs_Considered": "",
    "Selected_Source_URLs": "",
    "Tier": "",
    "Raw_Evidence": "",
    "Extraction_Logic": "",
    "Flags": ""
  }
}`;

                        const { url, init } = buildLLMRequest(
                            providerType, job.model_id, systemPrompt, userPrompt,
                            job.web_search_choice, conn.base_url, apiKey
                        );

                        const response = await fetchWithRetry(url, init);
                        const data = await response.json();
                        const content = extractTextContent(providerType, data);
                        let parsed = extractJSON(content);

                        if (!parsed) {
                            parsed = {
                                output: {
                                    Owner: input.Owner, Economy: input.Economy, Economy_Code: economyCode,
                                    Legal_basis: input.Legal_basis || input['Legal basis'],
                                    Question: input.Question, Topic: input.Topic,
                                    Extraction_Status: 'failed', Confidence_Score: 0,
                                    Processing_Notes: 'Failed to parse LLM response',
                                },
                                evidence: {
                                    Row_Index: row.row_index,
                                    Query_1: query1, Query_2: query2, Query_3: query3,
                                    Raw_Evidence: content.slice(0, 2000), Flags: 'PARSE_ERROR',
                                },
                            };
                        }

                        if (parsed.output) parsed.output.Economy_Code = economyCode;
                        if (!economyCode && parsed.evidence) {
                            parsed.evidence.Flags = [parsed.evidence.Flags, 'NO_ECONOMY_CODE'].filter(Boolean).join(', ');
                        }

                        await base44.entities.JobRow.update(row.id, {
                            status: 'done',
                            output_json: parsed.output || {},
                            evidence_json: parsed.evidence || {},
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

                await base44.entities.Job.update(job_id, {
                    processed_rows: doneCount + errCount,
                    status: newStatus,
                    progress_json: {
                        current_batch: (job.progress_json?.current_batch || 0) + 1,
                        last_row_index: pendingRows[pendingRows.length - 1]?.row_index || 0,
                    },
                });

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

                const oldRows = await base44.entities.JobRow.filter({ job_id });
                oldRows.sort((a, b) => a.row_index - b.row_index);

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

            default:
                return Response.json({ error: `Unknown action: ${action}` }, { status: 400 });
        }
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});