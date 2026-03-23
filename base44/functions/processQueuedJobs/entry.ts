import { createClientFromRequest } from 'npm:@base44/sdk@0.8.21';

// Maximum wall-clock milliseconds to spend in one automation invocation.
// Base44 automation functions have a ~25s CPU limit; we stop at 200s wall-clock
// to leave plenty of headroom for the final DB writes.
const MAX_WALL_MS = 200_000;
const BATCH_INTER_DELAY_MS = 1_500; // small pause between batches

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

Deno.serve(async (req) => {
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

            // Invoke the existing `process` action via the SDK.
            // Use retries with backoff to handle transient 403/5xx errors.
            let batchResp;
            const MAX_INVOKE_RETRIES = 3;
            let invokeSuccess = false;
            for (let attempt = 0; attempt < MAX_INVOKE_RETRIES; attempt++) {
                try {
                    if (attempt > 0) {
                        console.log(`[processQueuedJobs] Retry ${attempt} for batch ${batchesRun + 1}...`);
                        await sleep(2_000 * Math.pow(2, attempt));
                    } else {
                        console.log(`[processQueuedJobs] Invoking jobProcessor batch ${batchesRun + 1} for job ${job_id}...`);
                    }
                    const invokeResult = await sr.functions.invoke('jobProcessor', {
                        action: 'process',
                        job_id,
                    });
                    batchResp = invokeResult?.data || invokeResult;
                    console.log(`[processQueuedJobs] jobProcessor returned. remaining=${batchResp?.remaining}`);
                    invokeSuccess = true;
                    break;
                } catch (invokeErr) {
                    const errMsg = String(invokeErr?.message || invokeErr);
                    console.error(`[processQueuedJobs] invoke error (attempt ${attempt + 1}/${MAX_INVOKE_RETRIES}):`, errMsg);
                    // If the job is in error state, stop immediately
                    const afterErr = await sr.entities.Job.filter({ id: job_id });
                    if (afterErr[0]?.status === 'error' || afterErr[0]?.status === 'paused') {
                        lastResult = { status: afterErr[0].status };
                        invokeSuccess = false;
                        break;
                    }
                }
            }
            if (!invokeSuccess) {
                // All retries failed or job entered terminal state
                if (!lastResult) {
                    console.error(`[processQueuedJobs] All ${MAX_INVOKE_RETRIES} retries failed. Will retry on next automation tick.`);
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
            // Use a small delay then AWAIT the chain invocation to ensure it actually fires
            // before this function's response is sent back.
            await sleep(1_000);
            try {
                // Don't await the actual processing — just ensure the invoke request is sent
                sr.functions.invoke('processQueuedJobs', {}).catch((e) => {
                    console.error(`[processQueuedJobs] Chain invoke failed:`, e.message);
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