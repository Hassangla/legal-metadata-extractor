import { createClientFromRequest } from 'npm:@base44/sdk@0.8.21';

// Maximum wall-clock milliseconds to spend in one automation invocation.
// Base44 automation functions have a ~25s CPU limit; we stop at 200s wall-clock
// to leave plenty of headroom for the final DB writes.
const MAX_WALL_MS = 200_000;
const BATCH_INTER_DELAY_MS = 1_500; // small pause between batches

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Extract auth headers from the incoming request so we can forward them
// to jobProcessor via direct HTTP fetch (avoids 403 issues with sr.functions.invoke).
function extractAuthHeaders(req) {
    const headers = {};
    const auth = req.headers.get('authorization');
    if (auth) headers['Authorization'] = auth;
    const appId = req.headers.get('x-base44-app-id') || req.headers.get('x-app-id');
    if (appId) headers['x-base44-app-id'] = appId;
    // Forward any service-role related headers
    for (const [key, value] of req.headers.entries()) {
        const k = key.toLowerCase();
        if (k.startsWith('x-base44') || k === 'authorization') {
            headers[key] = value;
        }
    }
    headers['Content-Type'] = 'application/json';
    return headers;
}

Deno.serve(async (req) => {
    // Clone request body/headers before consuming
    const reqHeaders = extractAuthHeaders(req);
    const reqUrl = new URL(req.url);
    // Derive the base URL for sibling function calls
    const functionBaseUrl = `${reqUrl.protocol}//${reqUrl.host}`;

    try {
        const base44 = createClientFromRequest(req);

        // This function is called by the automation scheduler (no user session).
        // Use service role for all DB access.
        const sr = base44.asServiceRole;

        const startTime = Date.now();

        // Find the oldest queued or running job.
        // We process one job per invocation to keep things simple and predictable.
        const [queuedJobs, runningJobs] = await Promise.all([
            sr.entities.Job.filter({ status: 'queued' }, 'created_date', 1),
            sr.entities.Job.filter({ status: 'running' }, 'updated_date', 1),
        ]);

        // Prefer queued over running; among running, take the least-recently-updated one.
        const job = queuedJobs[0] || runningJobs[0];

        if (!job) {
            return Response.json({ message: 'No queued or running jobs found.' });
        }

        const job_id = job.id;
        console.log(`[processQueuedJobs] Picked up job ${job_id} (status=${job.status})`);

        // Delegate all processing to the existing jobProcessor `process` action.
        // We call it repeatedly until the job is done or we're close to the time limit.
        let batchesRun = 0;
        let lastResult = null;

        while (Date.now() - startTime < MAX_WALL_MS) {
            // Re-fetch the job to get the latest status before each batch.
            const freshJobs = await sr.entities.Job.filter({ id: job_id });
            if (!freshJobs.length) {
                console.log(`[processQueuedJobs] Job ${job_id} disappeared — stopping.`);
                break;
            }
            const freshJob = freshJobs[0];

            if (freshJob.status === 'done' || freshJob.status === 'error' || freshJob.status === 'paused') {
                console.log(`[processQueuedJobs] Job ${job_id} finished with status=${freshJob.status}.`);
                lastResult = { status: freshJob.status };
                break;
            }

            // Recover stale 'processing' rows that may have been left by a crashed batch.
            // If a row has been in 'processing' for too long, reset it to 'pending'.
            const processingRows = await sr.entities.JobRow.filter(
                { job_id, status: 'processing' },
                'row_index',
                50,
                0
            );
            for (const staleRow of processingRows) {
                try {
                    await sr.entities.JobRow.update(staleRow.id, { status: 'pending' });
                } catch (_) {}
            }

            // Check if there are any pending rows left.
            const pendingRows = await sr.entities.JobRow.filter(
                { job_id, status: 'pending' },
                'row_index',
                1,
                0
            );
            if (!pendingRows.length) {
                // No pending rows — mark done.
                await sr.entities.Job.update(job_id, {
                    status: 'done',
                    processed_rows: freshJob.total_rows || freshJob.processed_rows,
                });
                console.log(`[processQueuedJobs] Job ${job_id} — no pending rows, marked done.`);
                lastResult = { status: 'done' };
                break;
            }

            // Invoke jobProcessor via direct HTTP fetch to avoid 403 issues
            // with sr.functions.invoke when called from automation context.
            let batchResp;
            let invokeSuccess = false;
            console.log(`[processQueuedJobs] Invoking jobProcessor batch ${batchesRun + 1} for job ${job_id}...`);
            try {
                const resp = await fetch(`${functionBaseUrl}/jobProcessor`, {
                    method: 'POST',
                    headers: reqHeaders,
                    body: JSON.stringify({ action: 'process', job_id }),
                });
                if (!resp.ok) {
                    const errText = await resp.text();
                    throw new Error(`HTTP ${resp.status}: ${errText.slice(0, 300)}`);
                }
                batchResp = await resp.json();
                console.log(`[processQueuedJobs] jobProcessor returned. remaining=${batchResp?.remaining}`);
                invokeSuccess = true;
            } catch (invokeErr) {
                console.error(`[processQueuedJobs] invoke error on batch ${batchesRun + 1}:`, String(invokeErr?.message || invokeErr));
                // If the job is in error state, stop immediately
                const afterErr = await sr.entities.Job.filter({ id: job_id });
                if (afterErr[0]?.status === 'error' || afterErr[0]?.status === 'paused') {
                    lastResult = { status: afterErr[0].status };
                }
                break;
            }

            batchesRun++;
            const remaining = batchResp?.remaining;
            lastResult = batchResp;

            console.log(`[processQueuedJobs] Batch ${batchesRun} done. remaining=${remaining}`);

            if (remaining === 0 || remaining === undefined) {
                // jobProcessor already set status to 'done' — we're finished.
                break;
            }

            // Small inter-batch pause to avoid hammering the provider.
            if (Date.now() - startTime + BATCH_INTER_DELAY_MS < MAX_WALL_MS) {
                await sleep(BATCH_INTER_DELAY_MS);
            } else {
                break; // approaching time limit
            }
        }

        const elapsed = Math.round((Date.now() - startTime) / 1000);
        console.log(`[processQueuedJobs] Done. job=${job_id} batches=${batchesRun} elapsed=${elapsed}s`);

        // ── SELF-CHAIN: Ensure processing continues without waiting for the
        // next 5-minute automation tick. We schedule a continuation if there's
        // still work to do (current job or other jobs in queue).
        let shouldChain = false;

        // Check if the current job still needs work
        if (lastResult?.status !== 'done' && lastResult?.status !== 'error' && lastResult?.status !== 'paused') {
            shouldChain = true;
        }

        // Also check for other queued/running jobs
        if (!shouldChain) {
            try {
                const [otherQueued, otherRunning] = await Promise.all([
                    sr.entities.Job.filter({ status: 'queued' }, 'created_date', 1),
                    sr.entities.Job.filter({ status: 'running' }, 'updated_date', 1),
                ]);
                if (otherQueued.length || otherRunning.length) {
                    shouldChain = true;
                }
            } catch (_) {}
        }

        if (shouldChain) {
            console.log(`[processQueuedJobs] Self-chaining — more work remains.`);
            await sleep(1_000);
            try {
                // Fire-and-forget HTTP call to self to continue processing
                fetch(`${functionBaseUrl}/processQueuedJobs`, {
                    method: 'POST',
                    headers: reqHeaders,
                    body: JSON.stringify({}),
                }).catch((e) => {
                    console.error(`[processQueuedJobs] Chain fetch failed:`, e.message);
                });
                // Give the HTTP request time to leave before we return
                await sleep(500);
            } catch (chainErr) {
                console.error(`[processQueuedJobs] Failed to self-chain:`, chainErr.message);
            }
        }

        return Response.json({
            job_id,
            batches_run: batchesRun,
            elapsed_seconds: elapsed,
            last_result: lastResult,
            chained: shouldChain,
        });

    } catch (error) {
        console.error('[processQueuedJobs] Fatal error:', error.message);
        return Response.json({ error: error.message }, { status: 500 });
    }
});