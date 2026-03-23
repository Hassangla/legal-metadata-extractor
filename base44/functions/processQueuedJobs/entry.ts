import { createClientFromRequest } from 'npm:@base44/sdk@0.8.20';

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

            if (['done','error','paused','stopped','aborted'].includes(freshJob.status)) {
                console.log(`[processQueuedJobs] Job ${job_id} stopped with status=${freshJob.status}.`);
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

            // Invoke the existing `process` action via the SDK so all the complex
            // extraction / verification / pricing logic stays in one place.
            let batchResp;
            try {
                batchResp = await sr.functions.invoke('jobProcessor', {
                    action: 'process',
                    job_id,
                });
            } catch (invokeErr) {
                console.error(`[processQueuedJobs] invoke error on batch ${batchesRun + 1}:`, invokeErr.message);
                // If the jobProcessor itself set the job to error, we respect that and stop.
                const afterErr = await sr.entities.Job.filter({ id: job_id });
                if (afterErr[0]?.status === 'error') {
                    lastResult = { status: 'error', error: invokeErr.message };
                    break;
                }
                // Transient error — wait a bit and try again next automation tick
                break;
            }

            batchesRun++;
            lastResult = batchResp?.data || batchResp;
            const remaining = lastResult?.remaining ?? batchResp?.remaining;
            console.log(`[processQueuedJobs] Batch ${batchesRun} done. remaining=${remaining} status=${lastResult?.job?.status}`);
            if (remaining === 0 || remaining === undefined) break;
            // If process detected a stop/pause, stop immediately
            const jobSt = lastResult?.job?.status;
            if (jobSt && ['stopped','paused','done','error'].includes(jobSt)) break;

            // Small inter-batch pause to avoid hammering the provider.
            if (Date.now() - startTime + BATCH_INTER_DELAY_MS < MAX_WALL_MS) {
                await sleep(BATCH_INTER_DELAY_MS);
            } else {
                break; // approaching time limit
            }
        }

        const elapsed = Math.round((Date.now() - startTime) / 1000);
        console.log(`[processQueuedJobs] Done. job=${job_id} batches=${batchesRun} elapsed=${elapsed}s`);

        // Self-chain: if there are still queued/running jobs, schedule another
        // invocation so processing continues without relying on the frontend or
        // waiting for the next automation scheduler tick.
        //
        // IMPORTANT: We must *await* the invoke (with a timeout) instead of
        // fire-and-forget.  In serverless runtimes the execution context may be
        // terminated as soon as the Response is returned, so an un-awaited
        // outgoing HTTP request can be silently dropped — causing the chain to
        // break and jobs to stall.
        const lrSt = lastResult?.status || lastResult?.job?.status;
        const needsMoreWork = !['done','error','paused','stopped','aborted'].includes(lrSt) && batchesRun > 0;

        let shouldChain = needsMoreWork;

        if (!shouldChain) {
            // Check if there are other queued/running jobs waiting
            try {
                const [otherQueued, otherRunning] = await Promise.all([
                    sr.entities.Job.filter({ status: 'queued' }, 'created_date', 1),
                    sr.entities.Job.filter({ status: 'running' }, 'updated_date', 1),
                ]);
                shouldChain = otherQueued.length > 0 || otherRunning.length > 0;
            } catch (_) {}
        }

        if (shouldChain) {
            try {
                await sleep(2_000);
                // Await with a 10-second timeout so the HTTP request is guaranteed
                // to reach the server before this invocation ends.
                await Promise.race([
                    sr.functions.invoke('processQueuedJobs', {}),
                    sleep(10_000),
                ]);
                console.log(`[processQueuedJobs] Self-chained — more work remains.`);
            } catch (_) { /* non-fatal */ }
        }

        return Response.json({
            job_id,
            batches_run: batchesRun,
            elapsed_seconds: elapsed,
            last_result: lastResult,
        });

    } catch (error) {
        console.error('[processQueuedJobs] Fatal error:', error.message);
        return Response.json({ error: error.message }, { status: 500 });
    }
});