import { createClientFromRequest } from 'npm:@base44/sdk@0.8.21';

// Maximum wall-clock milliseconds to spend in one automation invocation.
const MAX_WALL_MS = 200_000;
const BATCH_INTER_DELAY_MS = 1_500;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const sr = base44.asServiceRole;
        const startTime = Date.now();

        // Find the oldest queued or running job.
        const [queuedJobs, runningJobs] = await Promise.all([
            sr.entities.Job.filter({ status: 'queued' }, 'created_date', 1),
            sr.entities.Job.filter({ status: 'running' }, 'updated_date', 1),
        ]);

        const job = queuedJobs[0] || runningJobs[0];
        if (!job) {
            return Response.json({ message: 'No queued or running jobs found.' });
        }

        const job_id = job.id;
        console.log(`[processQueuedJobs] Picked up job ${job_id} (status=${job.status})`);

        let batchesRun = 0;
        let lastResult = null;

        while (Date.now() - startTime < MAX_WALL_MS) {
            // Re-fetch job status before each batch
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

            // Recover stale 'processing' rows
            const processingRows = await sr.entities.JobRow.filter(
                { job_id, status: 'processing' }, 'row_index', 50, 0
            );
            if (processingRows.length > 0) {
                console.log(`[processQueuedJobs] Resetting ${processingRows.length} stale processing rows`);
                for (const staleRow of processingRows) {
                    try { await sr.entities.JobRow.update(staleRow.id, { status: 'pending' }); } catch (_) {}
                }
            }

            // Check for pending rows
            const pendingRows = await sr.entities.JobRow.filter(
                { job_id, status: 'pending' }, 'row_index', 1, 0
            );
            if (!pendingRows.length) {
                await sr.entities.Job.update(job_id, {
                    status: 'done',
                    processed_rows: freshJob.total_rows || freshJob.processed_rows,
                });
                console.log(`[processQueuedJobs] Job ${job_id} — no pending rows, marked done.`);
                lastResult = { status: 'done' };
                break;
            }

            // Invoke jobProcessor via SDK service role
            let batchResp;
            try {
                console.log(`[processQueuedJobs] Invoking jobProcessor batch ${batchesRun + 1}...`);
                const invokeResult = await sr.functions.invoke('jobProcessor', {
                    action: 'process',
                    job_id,
                });
                batchResp = invokeResult?.data || invokeResult;
                console.log(`[processQueuedJobs] Batch ${batchesRun + 1} done. remaining=${batchResp?.remaining}`);
            } catch (invokeErr) {
                console.error(`[processQueuedJobs] invoke error batch ${batchesRun + 1}:`, String(invokeErr?.message || invokeErr));
                // Check if job went to terminal state
                const afterErr = await sr.entities.Job.filter({ id: job_id });
                if (afterErr[0]?.status === 'error' || afterErr[0]?.status === 'paused') {
                    lastResult = { status: afterErr[0].status };
                    break;
                }
                // Transient error — break and let automation retry
                break;
            }

            batchesRun++;
            const remaining = batchResp?.remaining;
            lastResult = batchResp;

            if (remaining === 0 || remaining === undefined) break;

            if (Date.now() - startTime + BATCH_INTER_DELAY_MS < MAX_WALL_MS) {
                await sleep(BATCH_INTER_DELAY_MS);
            } else {
                break;
            }
        }

        const elapsed = Math.round((Date.now() - startTime) / 1000);
        console.log(`[processQueuedJobs] Done. job=${job_id} batches=${batchesRun} elapsed=${elapsed}s`);

        // Self-chain: check if more work remains
        let shouldChain = false;
        if (lastResult?.status !== 'done' && lastResult?.status !== 'error' && lastResult?.status !== 'paused') {
            shouldChain = true;
        }
        if (!shouldChain) {
            try {
                const [otherQueued, otherRunning] = await Promise.all([
                    sr.entities.Job.filter({ status: 'queued' }, 'created_date', 1),
                    sr.entities.Job.filter({ status: 'running' }, 'updated_date', 1),
                ]);
                if (otherQueued.length || otherRunning.length) shouldChain = true;
            } catch (_) {}
        }

        if (shouldChain) {
            console.log(`[processQueuedJobs] Self-chaining — more work remains.`);
            await sleep(1_000);
            try {
                sr.functions.invoke('processQueuedJobs', {}).catch(() => {});
                await sleep(500);
            } catch (_) {}
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