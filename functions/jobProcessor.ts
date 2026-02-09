import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

// ── AES-256-GCM crypto helpers ──────────────────────────────────

function getEncryptionKey() {
    const key = Deno.env.get("ENCRYPTION_KEY");
    if (!key) {
        throw new Error("ENCRYPTION_KEY environment variable is not set. Cannot decrypt API keys.");
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

async function decryptAndMigrate(conn, base44) {
    const plaintext = await decryptString(conn.api_key_encrypted);
    if (!conn.api_key_encrypted.includes(".")) {
        const encrypted = await encryptString(plaintext);
        await base44.entities.APIConnection.update(conn.id, { api_key_encrypted: encrypted });
    }
    return plaintext;
}

// ── In-memory lock (single-instance guard) ──────────────────────

const JOB_LOCKS = new Set();

// ── DB-level lock helpers ───────────────────────────────────────

const LOCK_TTL_MS = 2 * 60 * 1000; // 2 minutes

function generateToken() {
    return crypto.randomUUID();
}

async function acquireLock(base44, jobId) {
    const jobs = await base44.entities.Job.filter({ id: jobId });
    if (!jobs.length) return null;
    const job = jobs[0];

    // Check existing DB lock
    if (job.processing_lock_token && job.processing_lock_expires_at) {
        const expiresAt = new Date(job.processing_lock_expires_at).getTime();
        if (expiresAt > Date.now()) {
            return null; // Someone else holds the lock
        }
    }

    const token = generateToken();
    const expiresAt = new Date(Date.now() + LOCK_TTL_MS).toISOString();

    await base44.entities.Job.update(jobId, {
        processing_lock_token: token,
        processing_lock_expires_at: expiresAt
    });

    return token;
}

async function refreshLock(base44, jobId, token) {
    const expiresAt = new Date(Date.now() + LOCK_TTL_MS).toISOString();
    await base44.entities.Job.update(jobId, {
        processing_lock_token: token,
        processing_lock_expires_at: expiresAt
    });
}

async function releaseLock(base44, jobId) {
    await base44.entities.Job.update(jobId, {
        processing_lock_token: '',
        processing_lock_expires_at: ''
    });
}

// ── Row processing logic ────────────────────────────────────────

const BATCH_SIZE = 5;
const TIME_BUDGET_MS = 22000; // 22 seconds to leave headroom

async function processRow(row, conn, apiKey, job, specText, economyMap, base44) {
    await base44.entities.JobRow.update(row.id, { status: 'processing' });

    const input = row.input_data;
    const economyCode = economyMap[input.Economy?.toLowerCase()?.trim()] || '';

    const query1 = `${input.Legal_basis || input['Legal basis']} ${input.Economy} official text`;
    const query2 = `${input.Legal_basis || input['Legal basis']} ${input.Economy} legislation database`;
    const query3 = `${input.Topic} ${input.Economy} legal instrument ${input.Question}`;

    const prompt = `You are a legal metadata extraction assistant. Follow this specification exactly:

${specText}

Extract metadata for this row:
- Owner: ${input.Owner}
- Economy: ${input.Economy}
- Legal basis: ${input.Legal_basis || input['Legal basis']}
- Question: ${input.Question}
- Topic: ${input.Topic}

Search queries to use:
1. ${query1}
2. ${query2}
3. ${query3}

Return a JSON object with these exact fields:
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
    "Instrument_Date": "YYYY-MM-DD format",
    "Instrument_Type": "type of legal instrument",
    "Extraction_Status": "success/partial/failed",
    "Confidence_Score": 0.0-1.0,
    "Processing_Notes": "any notes"
  },
  "evidence": {
    "Row_Index": ${row.row_index},
    "Query_1": "${query1}",
    "Query_2": "${query2}",
    "Query_3": "${query3}",
    "URLs_Considered": "list of URLs checked",
    "Selected_Source_URLs": "chosen source URLs",
    "Tier": "1-4",
    "Raw_Evidence": "raw extracted text",
    "Extraction_Logic": "reasoning for extraction",
    "Flags": "any flags"
  }
}`;

    const requestBody = {
        model: job.model_id,
        messages: [
            { role: 'system', content: 'You are a legal metadata extraction assistant. Always respond with valid JSON.' },
            { role: 'user', content: prompt }
        ],
        max_tokens: 2000,
        temperature: 0.1
    };

    if (job.web_search_choice && job.web_search_choice !== 'none') {
        if (job.web_search_choice === 'web_search') {
            requestBody.tools = [{ type: 'web_search' }];
        }
    }

    const response = await fetch(`${conn.base_url}/v1/chat/completions`, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${apiKey}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`API error: ${response.status} - ${errorText}`);
    }

    const data = await response.json();
    const content = data.choices?.[0]?.message?.content || '';

    let parsed;
    try {
        const jsonMatch = content.match(/```(?:json)?\s*([\s\S]*?)```/) ||
                         content.match(/\{[\s\S]*\}/);
        const jsonStr = jsonMatch ? (jsonMatch[1] || jsonMatch[0]) : content;
        parsed = JSON.parse(jsonStr.trim());
    } catch (e) {
        parsed = {
            output: {
                Owner: input.Owner,
                Economy: input.Economy,
                Economy_Code: economyCode,
                Legal_basis: input.Legal_basis || input['Legal basis'],
                Question: input.Question,
                Topic: input.Topic,
                Extraction_Status: 'failed',
                Confidence_Score: 0,
                Processing_Notes: 'Failed to parse LLM response'
            },
            evidence: {
                Row_Index: row.row_index,
                Query_1: query1,
                Query_2: query2,
                Query_3: query3,
                Raw_Evidence: content,
                Flags: 'PARSE_ERROR'
            }
        };
    }

    if (!economyCode && parsed.evidence) {
        parsed.evidence.Flags = (parsed.evidence.Flags ? parsed.evidence.Flags + ', ' : '') + 'NO_ECONOMY_CODE';
    }

    await base44.entities.JobRow.update(row.id, {
        status: 'done',
        output_json: parsed.output || {},
        evidence_json: parsed.evidence || {}
    });
}

/**
 * Main server-side processing loop.
 * Processes batches within a time budget, updates progress after each batch.
 * Returns when time budget is exhausted or all rows are done.
 */
async function runProcessingLoop(base44, jobId, lockToken) {
    const startTime = Date.now();

    // Load shared context once
    const jobs = await base44.entities.Job.filter({ id: jobId });
    if (!jobs.length) return;
    const job = jobs[0];

    const connections = await base44.entities.APIConnection.filter({ id: job.connection_id });
    if (!connections.length) {
        await base44.entities.Job.update(jobId, { status: 'error', error_message: 'API connection not found' });
        return;
    }
    const conn = connections[0];
    const apiKey = await decryptAndMigrate(conn, base44);

    const specVersions = await base44.entities.SpecVersion.filter({ id: job.spec_version_id });
    const specText = specVersions[0]?.spec_text || '';

    const economyCodes = await base44.entities.EconomyCode.list();
    const economyMap = {};
    economyCodes.forEach(ec => {
        economyMap[ec.economy.toLowerCase().trim()] = ec.economy_code;
    });

    await base44.entities.Job.update(jobId, { status: 'running' });

    let totalProcessed = job.processed_rows || 0;
    let batchNumber = job.progress_json?.current_batch || 0;

    // Loop batches until time budget exhausted or done
    while (Date.now() - startTime < TIME_BUDGET_MS) {
        const allRows = await base44.entities.JobRow.filter({ job_id: jobId });
        const pendingRows = allRows
            .filter(r => r.status === 'pending')
            .sort((a, b) => a.row_index - b.row_index)
            .slice(0, BATCH_SIZE);

        if (pendingRows.length === 0) {
            // All done
            await base44.entities.Job.update(jobId, {
                status: 'done',
                processed_rows: job.total_rows || totalProcessed
            });
            return;
        }

        let batchProcessed = 0;
        for (const row of pendingRows) {
            // Check time budget before each row
            if (Date.now() - startTime >= TIME_BUDGET_MS) break;

            try {
                await processRow(row, conn, apiKey, job, specText, economyMap, base44);
                batchProcessed++;
            } catch (error) {
                await base44.entities.JobRow.update(row.id, {
                    status: 'error',
                    error_message: error.message
                });
            }
        }

        totalProcessed += batchProcessed;
        batchNumber++;

        // Refresh lock + update progress
        await refreshLock(base44, jobId, lockToken);
        await base44.entities.Job.update(jobId, {
            processed_rows: totalProcessed,
            progress_json: {
                current_batch: batchNumber,
                last_row_index: pendingRows[pendingRows.length - 1]?.row_index || 0
            }
        });
    }

    // Time budget exhausted but rows remain — mark as still running so frontend can re-kick
    const remainingRows = await base44.entities.JobRow.filter({ job_id: jobId });
    const pendingLeft = remainingRows.filter(r => r.status === 'pending').length;
    if (pendingLeft === 0) {
        await base44.entities.Job.update(jobId, {
            status: 'done',
            processed_rows: job.total_rows || totalProcessed
        });
    }
    // else: status stays 'running', frontend poll will call 'start' again
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
                const { 
                    connection_id, 
                    model_id, 
                    web_search_choice, 
                    input_file_url,
                    input_file_name,
                    total_rows,
                    input_rows 
                } = params;
                
                const specs = await base44.entities.Spec.filter({ is_active: true });
                if (specs.length === 0) {
                    return Response.json({ error: 'No active spec found. Please set up the spec first.' }, { status: 400 });
                }
                
                const versions = await base44.entities.SpecVersion.filter({ spec_id: specs[0].id });
                if (versions.length === 0) {
                    return Response.json({ error: 'No spec version found.' }, { status: 400 });
                }
                versions.sort((a, b) => (b.version_number || 0) - (a.version_number || 0));
                const latestVersion = versions[0];
                
                const connections = await base44.entities.APIConnection.filter({ id: connection_id });
                const connection = connections[0];
                
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
                    connection_name: connection?.name || 'Unknown',
                    model_name: model?.display_name || model_id,
                    processing_lock_token: '',
                    processing_lock_expires_at: ''
                });
                
                if (input_rows && input_rows.length > 0) {
                    for (let i = 0; i < input_rows.length; i++) {
                        await base44.entities.JobRow.create({
                            job_id: job.id,
                            row_index: i + 1,
                            input_data: input_rows[i],
                            status: 'pending'
                        });
                    }
                }

                // Auto-start processing: acquire lock and run
                if (JOB_LOCKS.has(job.id)) {
                    return Response.json({ job, message: 'Job created, processing already running' });
                }

                JOB_LOCKS.add(job.id);
                const lockToken = await acquireLock(base44, job.id);
                if (lockToken) {
                    try {
                        await runProcessingLoop(base44, job.id, lockToken);
                    } finally {
                        await releaseLock(base44, job.id);
                        JOB_LOCKS.delete(job.id);
                    }
                } else {
                    JOB_LOCKS.delete(job.id);
                }

                const updatedJobs = await base44.entities.Job.filter({ id: job.id });
                return Response.json({ job: updatedJobs[0] || job });
            }
            
            case 'start': {
                const { job_id } = params;
                
                const jobs = await base44.entities.Job.filter({ id: job_id });
                if (jobs.length === 0) {
                    return Response.json({ error: 'Job not found' }, { status: 404 });
                }
                
                const job = jobs[0];
                
                if (job.status === 'done') {
                    return Response.json({ job, message: 'Job already completed' });
                }
                if (job.status === 'error') {
                    return Response.json({ job, message: 'Job has errors' });
                }

                // In-memory lock check
                if (JOB_LOCKS.has(job_id)) {
                    return Response.json({ job, message: 'Already processing in this instance' });
                }

                // DB-level lock check
                const lockToken = await acquireLock(base44, job_id);
                if (!lockToken) {
                    return Response.json({ job, message: 'Already processing (locked by another instance)' });
                }

                JOB_LOCKS.add(job_id);
                try {
                    await runProcessingLoop(base44, job_id, lockToken);
                } finally {
                    await releaseLock(base44, job_id);
                    JOB_LOCKS.delete(job_id);
                }

                const updatedJobs = await base44.entities.Job.filter({ id: job_id });
                return Response.json({ job: updatedJobs[0] });
            }

            case 'getStatus': {
                const { job_id } = params;
                
                const jobs = await base44.entities.Job.filter({ id: job_id });
                if (jobs.length === 0) {
                    return Response.json({ error: 'Job not found' }, { status: 404 });
                }
                
                const job = jobs[0];
                const rows = await base44.entities.JobRow.filter({ job_id });
                
                const statusCounts = {
                    pending: rows.filter(r => r.status === 'pending').length,
                    processing: rows.filter(r => r.status === 'processing').length,
                    done: rows.filter(r => r.status === 'done').length,
                    error: rows.filter(r => r.status === 'error').length
                };
                
                return Response.json({ job, statusCounts });
            }
            
            case 'list': {
                const jobs = await base44.entities.Job.filter({ created_by: user.email });
                jobs.sort((a, b) => new Date(b.created_date) - new Date(a.created_date));
                return Response.json({ jobs });
            }
            
            case 'getRows': {
                const { job_id } = params;
                const rows = await base44.entities.JobRow.filter({ job_id });
                rows.sort((a, b) => a.row_index - b.row_index);
                return Response.json({ rows });
            }
            
            default:
                return Response.json({ error: 'Unknown action' }, { status: 400 });
        }
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});