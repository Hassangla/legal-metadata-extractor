import { createClientFromRequest } from 'npm:@base44/sdk@0.8.21';

// Base44 functions have a ~25s CPU limit. Keep wall-clock well under that.
// We process ONE batch per invocation and exit. The scheduler calls us every 5 min.
const MAX_WALL_MS = 18_000; // 18 seconds — safe margin
const LOCK_DURATION_MS = 5 * 60 * 1000; // 5 minutes — matches scheduler interval
const STALE_RUNNING_MS = 10 * 60 * 1000; // 10 minutes — recover stuck "running" jobs
const STALE_QUEUED_MS = 15 * 60 * 1000; // 15 minutes — recover orphaned "queued" jobs
const SELF_INVOKE_DELAY_MS = 1_000; // 1 second pause before self-invoke

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

Deno.serve(async (req) => {
    const startTime = Date.now();

    try {
        const base44 = createClientFromRequest(req);
        const sr = base44.asServiceRole;

        // ── STEP 0: Recover stale jobs ──
        // Jobs stuck in "running" with no progress for STALE_RUNNING_MS get requeued.
        // Jobs stuck in "queued" with expired locks get their locks cleared.
        try {
            const runningJobs = await sr.entities.Job.filter({ status: 'running' }, 'updated_date', 10);
            const now = new Date();
            for (const rj of runningJobs) {
                const updatedAt = new Date(rj.updated_date);
                const staleDuration = now.getTime() - updatedAt.getTime();
                if (staleDuration > STALE_RUNNING_MS) {
                    console.log(`[processQueuedJobs] Recovering stale running job ${rj.id} (stale ${Math.round(staleDuration / 1000)}s)`);
                    await sr.entities.Job.update(rj.id, {
                        status: 'queued',
                        processing_lock_token: null,
                        processing_lock_expires_at: null,
                        error_message: `Auto-requeued: no progress for ${Math.round(staleDuration / 60000)} min`,
                    });
                }
            }
        } catch (recoveryErr) {
            console.error('[processQueuedJobs] Recovery check error (non-fatal):', recoveryErr.message);
        }

        // ── STEP 1: Find a queued job to process ──
        const queuedJobs = await sr.entities.Job.filter({ status: 'queued' }, 'created_date', 5);
        
        if (!queuedJobs.length) {
            return Response.json({ message: 'No queued jobs found.' });
        }

        // Try to claim a job with atomic lock
        let claimedJob = null;
        const lockToken = crypto.randomUUID();
        const lockExpires = new Date(Date.now() + LOCK_DURATION_MS).toISOString();

        for (const candidate of queuedJobs) {
            // Skip if another worker has a valid lock
            if (candidate.processing_lock_token && candidate.processing_lock_expires_at) {
                const lockExp = new Date(candidate.processing_lock_expires_at);
                if (lockExp > new Date()) {
                    console.log(`[processQueuedJobs] Job ${candidate.id} already locked until ${candidate.processing_lock_expires_at} — skipping.`);
                    continue;
                }
                // Lock expired — we can claim it
                console.log(`[processQueuedJobs] Job ${candidate.id} has expired lock — reclaiming.`);
            }

            // Attempt to claim by writing our lock token
            try {
                await sr.entities.Job.update(candidate.id, {
                    processing_lock_token: lockToken,
                    processing_lock_expires_at: lockExpires,
                });

                // Re-read to verify we got the lock (optimistic concurrency)
                const [verified] = await sr.entities.Job.filter({ id: candidate.id });
                if (verified?.processing_lock_token === lockToken && verified?.status === 'queued') {
                    claimedJob = verified;
                    console.log(`[processQueuedJobs] Claimed job ${candidate.id} with lock ${lockToken}`);
                    break;
                } else {
                    console.log(`[processQueuedJobs] Lost claim race for job ${candidate.id} — trying next.`);
                }
            } catch (claimErr) {
                console.error(`[processQueuedJobs] Failed to claim job ${candidate.id}:`, claimErr.message);
            }
        }

        if (!claimedJob) {
            return Response.json({ message: 'No claimable queued jobs (all locked by other workers).' });
        }

        const job_id = claimedJob.id;

        // ── STEP 2: Process batches within time limit ──
        let batchesRun = 0;
        let lastResult = null;
        let shouldSelfInvoke = false;

        while (Date.now() - startTime < MAX_WALL_MS) {
            // Re-fetch job to check for external status changes (pause/cancel)
            const [freshJob] = await sr.entities.Job.filter({ id: job_id });
            if (!freshJob) {
                console.log(`[processQueuedJobs] Job ${job_id} disappeared — stopping.`);
                break;
            }

            if (['done', 'error', 'cancelled', 'paused'].includes(freshJob.status)) {
                console.log(`[processQueuedJobs] Job ${job_id} is now ${freshJob.status} — stopping.`);
                lastResult = { status: freshJob.status };
                break;
            }

            // Check if there are any pending rows
            const pendingRows = await sr.entities.JobRow.filter(
                { job_id, status: 'pending' }, 'row_index', 1, 0
            );
            if (!pendingRows.length) {
                // Check if any rows are still "processing" — don't mark done prematurely
                const processingRows = await sr.entities.JobRow.filter(
                    { job_id, status: 'processing' }, 'row_index', 1, 0
                );
                if (processingRows.length === 0) {
                    await sr.entities.Job.update(job_id, {
                        status: 'done',
                        processed_rows: freshJob.total_rows || freshJob.processed_rows,
                        processing_lock_token: null,
                        processing_lock_expires_at: null,
                    });
                    console.log(`[processQueuedJobs] Job ${job_id} — no pending/processing rows, marked done.`);
                    lastResult = { status: 'done' };
                } else {
                    console.log(`[processQueuedJobs] Job ${job_id} — ${processingRows.length} rows still processing, not marking done yet.`);
                    lastResult = { status: 'waiting_for_processing_rows' };
                    shouldSelfInvoke = true; // re-check soon
                }
                break;
            }

            // Invoke jobProcessor.process — use base44 (request-scoped client)
            // instead of sr.functions.invoke which gets 403 on this platform.
            let batchResp;
            try {
                batchResp = await base44.functions.invoke('jobProcessor', {
                    action: 'process',
                    job_id,
                    _service_call: true,
                });
            } catch (invokeErr) {
                console.error(`[processQueuedJobs] invoke error on batch ${batchesRun + 1}:`, invokeErr.message);
                const [afterErr] = await sr.entities.Job.filter({ id: job_id });
                if (afterErr?.status === 'error') {
                    lastResult = { status: 'error', error: invokeErr.message };
                    break;
                }
                // Transient error — stop this invocation, next scheduler tick will retry
                lastResult = { error: invokeErr.message };
                shouldSelfInvoke = true; // try again soon rather than waiting 5 min
                break;
            }

            batchesRun++;
            const remaining = batchResp?.remaining ?? batchResp?.data?.remaining;
            lastResult = batchResp?.data || batchResp;

            console.log(`[processQueuedJobs] Batch ${batchesRun} done. remaining=${remaining}`);

            if (remaining === 0 || remaining === undefined) {
                break;
            }

            // There's more work — we'll self-invoke after this invocation ends
            shouldSelfInvoke = true;

            // Check time before another batch
            if (Date.now() - startTime + 2000 >= MAX_WALL_MS) {
                console.log(`[processQueuedJobs] Approaching time limit — exiting loop.`);
                break;
            }

            await sleep(500);
        }

        // ── STEP 3: Release lock ──
        try {
            const [finalJob] = await sr.entities.Job.filter({ id: job_id });
            if (finalJob?.processing_lock_token === lockToken) {
                await sr.entities.Job.update(job_id, {
                    processing_lock_token: null,
                    processing_lock_expires_at: null,
                });
            }
        } catch (_) { /* non-fatal */ }

        const elapsed = Math.round((Date.now() - startTime) / 1000);
        console.log(`[processQueuedJobs] Done. job=${job_id} batches=${batchesRun} elapsed=${elapsed}s`);

        // ── STEP 4: Self-invoke if more work remains ──
        // This keeps jobs processing continuously without depending on the
        // 5-minute scheduler or the browser staying open.
        if (!shouldSelfInvoke) {
            // Even if this job is done, check if other queued jobs exist
            try {
                const moreQueued = await sr.entities.Job.filter({ status: 'queued' }, 'created_date', 1);
                if (moreQueued.length > 0) {
                    shouldSelfInvoke = true;
                }
            } catch (_) { /* non-fatal */ }
        }

        if (shouldSelfInvoke) {
            // Fire-and-forget: delay slightly then re-invoke ourselves.
            // If this fails, the 5-min scheduler is the safety net.
            try {
                await sleep(SELF_INVOKE_DELAY_MS);
                // Don't await the full response — just kick it off
                base44.functions.invoke('processQueuedJobs', { _self_invoke: true }).catch((err) => {
                    console.warn(`[processQueuedJobs] Self-invoke failed (scheduler will retry): ${err.message}`);
                });
                console.log(`[processQueuedJobs] Self-invoked for continued processing.`);
            } catch (selfErr) {
                // Non-fatal — scheduler will pick it up in ≤5 min
                console.warn(`[processQueuedJobs] Self-invoke failed (scheduler will retry): ${selfErr.message}`);
            }
        }

        return Response.json({
            job_id,
            batches_run: batchesRun,
            elapsed_seconds: elapsed,
            last_result: lastResult,
            self_invoked: shouldSelfInvoke,
        });

    } catch (error) {
        console.error('[processQueuedJobs] Fatal error:', error.message);
        return Response.json({ error: error.message }, { status: 500 });
    }
});