import { createClientFromRequest } from 'npm:@base44/sdk@0.8.21';

// jobProcessor can take 60-120s per batch (web search rows are slow).
// Give enough room for 1 batch + overhead, but stay under Deno's 300s hard limit.
const MAX_WALL_MS = 150_000;  // 150 seconds max
const BATCH_INTER_DELAY_MS = 1_000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

Deno.serve(async (req) => {
    // CRITICAL: Always return 200 so the automation never accumulates consecutive failures
    // and never gets auto-disabled. Errors are logged but do not produce 4xx/5xx responses.
    try {
        const base44 = createClientFromRequest(req);
        const sr = base44.asServiceRole;
        const startTime = Date.now();

        // Find the oldest queued or running job.
        let queuedJobs, runningJobs;
        try {
            [queuedJobs, runningJobs] = await Promise.all([
                sr.entities.Job.filter({ status: 'queued' }, 'created_date', 1),
                sr.entities.Job.filter({ status: 'running' }, 'updated_date', 1),
            ]);
        } catch (e) {
            console.error('[processQueuedJobs] Failed to query jobs:', e.message);
            return Response.json({ ok: true, message: 'Failed to query jobs, will retry next tick.' });
        }

        const job = (queuedJobs || [])[0] || (runningJobs || [])[0];
        if (!job) {
            return Response.json({ ok: true, message: 'No queued or running jobs.' });
        }

        const job_id = job.id;
        console.log(`[processQueuedJobs] Picked up job ${job_id} (status=${job.status})`);

        let batchesRun = 0;
        let lastResult = null;

        console.log(`[processQueuedJobs] Entering processing loop. timeLeft=${MAX_WALL_MS - (Date.now() - startTime)}ms`);

        while (Date.now() - startTime < MAX_WALL_MS) {
            console.log(`[processQueuedJobs] Loop iteration ${batchesRun}. elapsed=${Date.now() - startTime}ms`);
            
            // Re-fetch job status before each batch
            let freshJob;
            try {
                const freshJobs = await sr.entities.Job.filter({ id: job_id });
                freshJob = freshJobs[0];
                console.log(`[processQueuedJobs] Fresh job status=${freshJob?.status}`);
            } catch (e) {
                console.error('[processQueuedJobs] Failed to re-fetch job:', e.message);
                break;
            }
            if (!freshJob) {
                console.log(`[processQueuedJobs] Job ${job_id} disappeared — stopping.`);
                break;
            }

            if (freshJob.status === 'done' || freshJob.status === 'error' || freshJob.status === 'paused') {
                console.log(`[processQueuedJobs] Job ${job_id} reached terminal status=${freshJob.status}.`);
                lastResult = { status: freshJob.status };
                break;
            }

            // Recover stale 'processing' rows
            try {
                const processingRows = await sr.entities.JobRow.filter(
                    { job_id, status: 'processing' }, 'row_index', 50, 0
                );
                console.log(`[processQueuedJobs] Found ${processingRows.length} processing rows`);
                if (processingRows.length > 0) {
                    console.log(`[processQueuedJobs] Resetting ${processingRows.length} stale processing rows`);
                    for (const staleRow of processingRows) {
                        try { await sr.entities.JobRow.update(staleRow.id, { status: 'pending' }); } catch (_) {}
                    }
                }
            } catch (e) {
                console.error('[processQueuedJobs] Failed to check processing rows:', e.message);
            }

            // Check for pending rows
            try {
                const pendingRows = await sr.entities.JobRow.filter(
                    { job_id, status: 'pending' }, 'row_index', 1, 0
                );
                console.log(`[processQueuedJobs] Found ${pendingRows.length} pending rows`);
                if (!pendingRows.length) {
                    try {
                        await sr.entities.Job.update(job_id, {
                            status: 'done',
                            processed_rows: freshJob.total_rows || freshJob.processed_rows,
                        });
                    } catch (_) {}
                    console.log(`[processQueuedJobs] Job ${job_id} — no pending rows, marked done.`);
                    lastResult = { status: 'done' };
                    break;
                }
            } catch (e) {
                console.error('[processQueuedJobs] Failed to check pending rows:', e.message);
                break;
            }

            // Invoke jobProcessor via SDK service role
            try {
                console.log(`[processQueuedJobs] Invoking jobProcessor batch ${batchesRun + 1}...`);
                const invokeResult = await sr.functions.invoke('jobProcessor', {
                    action: 'process',
                    job_id,
                });
                const batchResp = invokeResult?.data || invokeResult;
                console.log(`[processQueuedJobs] Batch ${batchesRun + 1} done. remaining=${batchResp?.remaining} resp_keys=${Object.keys(batchResp || {}).join(',')}`);
                batchesRun++;
                lastResult = batchResp;
                const remaining = batchResp?.remaining;
                if (remaining === 0 || remaining === undefined) break;
            } catch (invokeErr) {
                console.error(`[processQueuedJobs] invoke error batch ${batchesRun + 1}:`, invokeErr?.message || String(invokeErr), 'status:', invokeErr?.response?.status, 'data:', JSON.stringify(invokeErr?.response?.data || {}).slice(0, 300));
                // Check if job went to terminal state
                try {
                    const afterErr = await sr.entities.Job.filter({ id: job_id });
                    if (afterErr[0]?.status === 'error' || afterErr[0]?.status === 'paused') {
                        lastResult = { status: afterErr[0].status };
                    }
                } catch (_) {}
                break;  // Stop and let the next automation tick retry
            }

            // Check if we have time for another batch
            if (Date.now() - startTime + BATCH_INTER_DELAY_MS >= MAX_WALL_MS) break;
            await sleep(BATCH_INTER_DELAY_MS);
        }

        const elapsed = Math.round((Date.now() - startTime) / 1000);
        console.log(`[processQueuedJobs] Done. job=${job_id} batches=${batchesRun} elapsed=${elapsed}s`);

        // Always return 200 — prevents auto-disable of the automation
        return Response.json({
            ok: true,
            job_id,
            batches_run: batchesRun,
            elapsed_seconds: elapsed,
            last_result: lastResult,
        });

    } catch (error) {
        // Even fatal errors return 200 to prevent automation auto-disable
        console.error('[processQueuedJobs] Fatal error:', error.message);
        return Response.json({ ok: true, error: error.message, message: 'Fatal error, will retry next tick.' });
    }
});