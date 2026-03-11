import { createClientFromRequest } from 'npm:@base44/sdk@0.8.20';

// Budget: leave ~30s headroom before Deno's execution limit
const MAX_RUNTIME_MS = 4 * 60 * 1000; // 4 minutes
const BATCH_INTER_DELAY_MS = 2000;     // 2s between batches
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

Deno.serve(async (req) => {
    const startTime = Date.now();

    try {
        const base44 = createClientFromRequest(req);

        // Auth: accept both user-initiated calls and automation (no user token)
        let isAutomation = false;
        try {
            const user = await base44.auth.me();
            if (user && user.role !== 'admin') {
                return Response.json({ error: 'Forbidden' }, { status: 403 });
            }
        } catch (_) {
            // Automation calls have no user token — treat as trusted
            isAutomation = true;
        }

        const serviceBase44 = base44.asServiceRole;

        // Find one active job: prefer 'queued', then 'running'
        let activeJob = null;
        const queued = await serviceBase44.entities.Job.filter({ status: 'queued' }, '-created_date', 1);
        if (queued.length > 0) {
            activeJob = queued[0];
        } else {
            const running = await serviceBase44.entities.Job.filter({ status: 'running' }, '-updated_date', 1);
            if (running.length > 0) {
                activeJob = running[0];
            }
        }

        if (!activeJob) {
            return Response.json({ message: 'No queued or running jobs found', batches_processed: 0 });
        }

        const jobId = activeJob.id;
        let batchesProcessed = 0;
        let lastStatus = activeJob.status;

        console.log(`[processQueuedJobs] Starting job ${jobId} (status=${activeJob.status}, rows=${activeJob.total_rows})`);

        // Process batches until time limit or job completes
        while (Date.now() - startTime < MAX_RUNTIME_MS) {
            // Check if there are still pending rows
            const pendingRows = await serviceBase44.entities.JobRow.filter(
                { job_id: jobId, status: 'pending' },
                'row_index',
                1,
                0
            );

            if (pendingRows.length === 0) {
                // No pending rows — check for stuck 'processing' rows
                const processingRows = await serviceBase44.entities.JobRow.filter(
                    { job_id: jobId, status: 'processing' },
                    'row_index',
                    1,
                    0
                );
                if (processingRows.length === 0) {
                    // All done — mark job complete if not already
                    const latestJobs = await serviceBase44.entities.Job.filter({ id: jobId });
                    if (latestJobs.length > 0 && latestJobs[0].status !== 'done' && latestJobs[0].status !== 'error') {
                        await serviceBase44.entities.Job.update(jobId, { status: 'done' });
                        lastStatus = 'done';
                        console.log(`[processQueuedJobs] Job ${jobId} marked done (no pending rows)`);
                    }
                }
                break;
            }

            // Invoke jobProcessor's process action
            try {
                const result = await serviceBase44.functions.invoke('jobProcessor', {
                    action: 'process',
                    job_id: jobId,
                });

                batchesProcessed++;
                const remaining = result?.remaining ?? pendingRows.length;
                lastStatus = result?.job?.status ?? 'running';

                console.log(`[processQueuedJobs] Batch ${batchesProcessed} done. remaining=${remaining} status=${lastStatus}`);

                if (lastStatus === 'done' || lastStatus === 'error') {
                    break;
                }

                if (remaining <= 0) {
                    break;
                }
            } catch (batchErr) {
                console.error(`[processQueuedJobs] Batch error: ${batchErr.message}`);
                // If rate limited, break and let next automation run pick it up
                if (/429|rate.?limit/i.test(batchErr.message || '')) {
                    console.log(`[processQueuedJobs] Rate limited — stopping early, will resume next run`);
                    break;
                }
                // Non-retryable error — mark job error
                await serviceBase44.entities.Job.update(jobId, {
                    status: 'error',
                    error_message: `Worker error: ${batchErr.message?.slice(0, 400)}`,
                });
                lastStatus = 'error';
                break;
            }

            // Check time budget before next batch
            if (Date.now() - startTime >= MAX_RUNTIME_MS) {
                console.log(`[processQueuedJobs] Time limit reached after ${batchesProcessed} batches — will resume next run`);
                break;
            }

            await sleep(BATCH_INTER_DELAY_MS);
        }

        const elapsed = Math.round((Date.now() - startTime) / 1000);
        return Response.json({
            job_id: jobId,
            batches_processed: batchesProcessed,
            final_status: lastStatus,
            elapsed_seconds: elapsed,
            is_automation: isAutomation,
        });

    } catch (error) {
        console.error(`[processQueuedJobs] Fatal error: ${error.message}`);
        return Response.json({ error: error.message }, { status: 500 });
    }
});