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

const LOCK_TTL_MS = 2 * 60 * 1000;

function generateToken() {
    return crypto.randomUUID();
}

async function acquireLock(base44, jobId) {
    const jobs = await base44.entities.Job.filter({ id: jobId });
    if (!jobs.length) return null;
    const job = jobs[0];

    if (job.processing_lock_token && job.processing_lock_expires_at) {
        const expiresAt = new Date(job.processing_lock_expires_at).getTime();
        if (expiresAt > Date.now()) {
            return null;
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

// ── Retry helpers ───────────────────────────────────────────────

const MAX_RETRIES = 3;
const BACKOFF_BASE_MS = 1000;
const BACKOFF_CAP_MS = 60000;

function isRetryableError(error, httpStatus) {
    if (httpStatus === 429) return true;
    if (httpStatus >= 500 && httpStatus <= 599) return true;
    // Network / fetch errors
    if (!httpStatus && error) return true;
    return false;
}

function computeBackoffMs(retryCount) {
    const exponential = BACKOFF_BASE_MS * Math.pow(2, retryCount);
    const capped = Math.min(exponential, BACKOFF_CAP_MS);
    const jitter = Math.floor(Math.random() * 500);
    return capped + jitter;
}

// ── Row processing logic ────────────────────────────────────────

const BATCH_SIZE = 5;
const TIME_BUDGET_MS = 22000;

async function processRow(row, conn, apiKey, job, specText, economyMap, base44) {
    await base44.entities.JobRow.update(row.id, { status: 'processing' });

    const input = row.input_data;
    const economyCode = economyMap[input.Economy?.toLowerCase()?.trim()] || '';

    const query1 = `${input.Legal_basis || input['Legal basis']} ${input.Economy} official text`;
    const query2 = `${input.Legal_basis || input['Legal basis']} ${input.Economy} legislation database`;
    const query3 = `${input.Topic} ${input.Economy} legal instrument ${input.Question}`;

    const systemPrompt = `You are a legal metadata extraction assistant. You MUST return ONLY valid JSON with no extra text, no markdown, no code fences. Return a JSON object with exactly two keys: "output" and "evidence".`;

    const userPrompt = `SPEC TEXT (authoritative):
${specText}

ROW INPUT:
- Owner: ${input.Owner}
- Economy: ${input.Economy}
- Legal_basis: ${input.Legal_basis || input['Legal basis']}
- Question: ${input.Question}
- Topic: ${input.Topic}

SEARCH QUERIES TO USE:
1. ${query1}
2. ${query2}
3. ${query3}

Return JSON with keys "output" and "evidence":
{
  "output": {
    "Owner": "${input.Owner}",
    "Economy": "${input.Economy}",
    "Legal_basis": "${input.Legal_basis || input['Legal basis']}",
    "Question": "${input.Question}",
    "Topic": "${input.Topic}",
    "Instrument_Title": "extracted title",
    "Instrument_URL": "source URL",
    "Instrument_Date": "YYYY-MM-DD format",
    "Instrument_Type": "type of legal instrument",
    "Extraction_Status": "success or partial or failed",
    "Confidence_Score": 0.0,
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
}

No extra text. No markdown. No code fences. ONLY the JSON object.`;

    const requestBody = {
        model: job.model_id,
        messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: userPrompt }
        ],
        max_tokens: 2000,
        temperature: 0,
        response_format: { type: 'json_object' }
    };

    // Only attach web search tools if the model catalog confirmed support
    if (job.web_search_choice && job.web_search_choice !== 'none') {
        // We only attach if the probe confirmed this exact tool type
        if (job.web_search_choice === 'web_search') {
            requestBody.tools = [{ type: 'web_search' }];
        }
        // For other provider-specific options, don't attach tools —
        // those providers handle it via model capabilities natively
    }

    let response;
    let httpStatus;
    try {
        response = await fetch(`${conn.base_url}/v1/chat/completions`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${apiKey}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestBody)
        });
        httpStatus = response.status;
    } catch (fetchError) {
        // Network error — retryable
        throw { message: fetchError.message, httpStatus: 0, retryable: true };
    }

    if (!response.ok) {
        const errorText = await response.text();
        const retryable = isRetryableError(null, httpStatus);

        // If response_format not supported, retry without it
        if (httpStatus === 400 && errorText.includes('response_format')) {
            // Fallback: remove response_format and retry once
            delete requestBody.response_format;
            const retryResp = await fetch(`${conn.base_url}/v1/chat/completions`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${apiKey}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(requestBody)
            });
            if (!retryResp.ok) {
                const retryText = await retryResp.text();
                throw { message: `API error: ${retryResp.status} - ${retryText}`, httpStatus: retryResp.status, retryable: isRetryableError(null, retryResp.status) };
            }
            response = retryResp;
        } else {
            throw { message: `API error: ${httpStatus} - ${errorText}`, httpStatus, retryable };
        }
    }

    const data = await response.json();
    const content = data.choices?.[0]?.message?.content || '';

    let parsed;
    try {
        parsed = JSON.parse(content);
    } catch (e) {
        // Fallback: try extracting JSON from possible code fences
        const jsonMatch = content.match(/```(?:json)?\s*([\s\S]*?)```/) ||
                         content.match(/\{[\s\S]*\}/);
        if (jsonMatch) {
            const jsonStr = jsonMatch[1] || jsonMatch[0];
            parsed = JSON.parse(jsonStr.trim());
        } else {
            // Store raw response and mark error
            await base44.entities.JobRow.update(row.id, {
                status: 'error',
                error_message: 'Failed to parse LLM response as JSON',
                evidence_json: { Row_Index: row.row_index, Raw_Evidence: content, Flags: 'PARSE_ERROR' },
                retry_count: MAX_RETRIES,
                last_error_code: 0
            });
            return;
        }
    }

    // Post-process: inject economy code (not from model)
    if (parsed.output) {
        parsed.output.Economy_Code = economyCode;
    }

    // Flag if economy code is missing
    if (!economyCode && parsed.evidence) {
        parsed.evidence.Flags = (parsed.evidence.Flags ? parsed.evidence.Flags + ', ' : '') + 'NO_ECONOMY_CODE';
    }

    await base44.entities.JobRow.update(row.id, {
        status: 'done',
        output_json: parsed.output || {},
        evidence_json: parsed.evidence || {},
        error_message: ''
    });
}

/**
 * Get eligible rows: pending, or retryable errors whose backoff has elapsed.
 */
function getEligibleRows(allRows) {
    const now = Date.now();
    return allRows.filter(r => {
        if (r.status === 'pending') return true;
        if (r.status === 'error') {
            const retryCount = r.retry_count || 0;
            if (retryCount >= MAX_RETRIES) return false;
            const nextRetryAt = r.next_retry_at ? new Date(r.next_retry_at).getTime() : 0;
            return nextRetryAt <= now;
        }
        return false;
    }).sort((a, b) => a.row_index - b.row_index);
}

/**
 * Main server-side processing loop.
 */
async function runProcessingLoop(base44, jobId, lockToken) {
    const startTime = Date.now();

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

    while (Date.now() - startTime < TIME_BUDGET_MS) {
        const allRows = await base44.entities.JobRow.filter({ job_id: jobId });
        const eligibleRows = getEligibleRows(allRows);
        const batch = eligibleRows.slice(0, BATCH_SIZE);

        if (batch.length === 0) {
            // Check if truly done or just waiting for retries
            const stillRetrying = allRows.filter(r =>
                r.status === 'error' && (r.retry_count || 0) < MAX_RETRIES
            );
            if (stillRetrying.length === 0) {
                const doneCount = allRows.filter(r => r.status === 'done').length;
                await base44.entities.Job.update(jobId, {
                    status: 'done',
                    processed_rows: doneCount
                });
                return;
            }
            // Rows exist but not yet eligible (backoff pending) — return, frontend will re-kick
            break;
        }

        let batchProcessed = 0;
        for (const row of batch) {
            if (Date.now() - startTime >= TIME_BUDGET_MS) break;

            try {
                await processRow(row, conn, apiKey, job, specText, economyMap, base44);
                batchProcessed++;
            } catch (error) {
                const httpStatus = error.httpStatus || 0;
                const retryable = error.retryable !== undefined ? error.retryable : isRetryableError(error, httpStatus);
                const retryCount = (row.retry_count || 0) + 1;

                if (retryable && retryCount < MAX_RETRIES) {
                    const backoffMs = computeBackoffMs(retryCount);
                    await base44.entities.JobRow.update(row.id, {
                        status: 'pending',
                        retry_count: retryCount,
                        next_retry_at: new Date(Date.now() + backoffMs).toISOString(),
                        last_error_code: httpStatus,
                        error_message: error.message || String(error)
                    });
                } else {
                    await base44.entities.JobRow.update(row.id, {
                        status: 'error',
                        retry_count: retryCount,
                        last_error_code: httpStatus,
                        error_message: error.message || String(error)
                    });
                }
            }
        }

        totalProcessed += batchProcessed;
        batchNumber++;

        await refreshLock(base44, jobId, lockToken);

        const latestRows = await base44.entities.JobRow.filter({ job_id: jobId });
        const doneCount = latestRows.filter(r => r.status === 'done').length;

        await base44.entities.Job.update(jobId, {
            processed_rows: doneCount,
            progress_json: {
                current_batch: batchNumber,
                last_row_index: batch[batch.length - 1]?.row_index || 0
            }
        });
    }

    // Time budget exhausted — check final state
    const finalRows = await base44.entities.JobRow.filter({ job_id: jobId });
    const eligible = getEligibleRows(finalRows);
    const stillRetrying = finalRows.filter(r =>
        r.status === 'error' && (r.retry_count || 0) < MAX_RETRIES
    );

    if (eligible.length === 0 && stillRetrying.length === 0) {
        const doneCount = finalRows.filter(r => r.status === 'done').length;
        await base44.entities.Job.update(jobId, {
            status: 'done',
            processed_rows: doneCount
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
                            status: 'pending',
                            retry_count: 0
                        });
                    }
                }

                // Auto-start processing
                if (!JOB_LOCKS.has(job.id)) {
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
                }

                const updatedJobs = await base44.entities.Job.filter({ id: job.id });
                return Response.json({ job: updatedJobs[0] || job });
            }
            
            case 'start': {
                const { job_id } = params;
                
                if (!job_id) {
                    return Response.json({ error: 'job_id is required' }, { status: 400 });
                }

                const jobs = await base44.entities.Job.filter({ id: job_id });
                if (jobs.length === 0) {
                    return Response.json({ error: 'Job not found' }, { status: 404 });
                }
                
                const job = jobs[0];
                
                if (job.status === 'done') {
                    return Response.json({ job, message: 'Job already completed' });
                }

                if (JOB_LOCKS.has(job_id)) {
                    return Response.json({ job, message: 'Already processing in this instance' });
                }

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

            case 'rerun': {
                const { job_id, use_latest_spec } = params;

                if (!job_id) {
                    return Response.json({ error: 'job_id is required' }, { status: 400 });
                }

                // Load old job
                const oldJobs = await base44.entities.Job.filter({ id: job_id });
                if (!oldJobs.length) {
                    return Response.json({ error: 'Original job not found' }, { status: 404 });
                }
                const oldJob = oldJobs[0];

                // Determine spec version
                let specVersionId = oldJob.spec_version_id;
                if (use_latest_spec) {
                    const specs = await base44.entities.Spec.filter({ is_active: true });
                    if (specs.length > 0) {
                        const versions = await base44.entities.SpecVersion.filter({ spec_id: specs[0].id });
                        versions.sort((a, b) => (b.version_number || 0) - (a.version_number || 0));
                        if (versions.length > 0) {
                            specVersionId = versions[0].id;
                        }
                    }
                }

                // Get old rows for input data
                const oldRows = await base44.entities.JobRow.filter({ job_id });
                oldRows.sort((a, b) => a.row_index - b.row_index);

                if (oldRows.length === 0) {
                    // Fall back to re-parsing input file
                    const parseResp = await base44.functions.invoke('parseInputFile', { file_url: oldJob.input_file_url });
                    if (parseResp.rows && parseResp.rows.length > 0) {
                        oldRows.push(...parseResp.rows.map((r, i) => ({ input_data: r, row_index: i + 1 })));
                    }
                }

                const connections = await base44.entities.APIConnection.filter({ id: oldJob.connection_id });
                const models = await base44.entities.ModelCatalog.filter({ connection_id: oldJob.connection_id, model_id: oldJob.model_id });

                // Create new job
                const newJob = await base44.entities.Job.create({
                    connection_id: oldJob.connection_id,
                    model_id: oldJob.model_id,
                    web_search_choice: oldJob.web_search_choice || 'none',
                    spec_version_id: specVersionId,
                    status: 'queued',
                    input_file_url: oldJob.input_file_url,
                    input_file_name: oldJob.input_file_name,
                    total_rows: oldRows.length,
                    processed_rows: 0,
                    progress_json: { current_batch: 0, last_row_index: 0 },
                    connection_name: connections[0]?.name || oldJob.connection_name || 'Unknown',
                    model_name: models[0]?.display_name || oldJob.model_name || oldJob.model_id,
                    processing_lock_token: '',
                    processing_lock_expires_at: ''
                });

                // Create new rows
                for (const row of oldRows) {
                    await base44.entities.JobRow.create({
                        job_id: newJob.id,
                        row_index: row.row_index,
                        input_data: row.input_data,
                        status: 'pending',
                        retry_count: 0
                    });
                }

                // Auto-start
                if (!JOB_LOCKS.has(newJob.id)) {
                    JOB_LOCKS.add(newJob.id);
                    const lockToken = await acquireLock(base44, newJob.id);
                    if (lockToken) {
                        try {
                            await runProcessingLoop(base44, newJob.id, lockToken);
                        } finally {
                            await releaseLock(base44, newJob.id);
                            JOB_LOCKS.delete(newJob.id);
                        }
                    } else {
                        JOB_LOCKS.delete(newJob.id);
                    }
                }

                const updatedJobs = await base44.entities.Job.filter({ id: newJob.id });
                return Response.json({ job: updatedJobs[0] || newJob });
            }

            case 'getStatus': {
                const { job_id } = params;
                
                if (!job_id) {
                    return Response.json({ error: 'job_id is required' }, { status: 400 });
                }

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
                if (!job_id) {
                    return Response.json({ error: 'job_id is required' }, { status: 400 });
                }
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