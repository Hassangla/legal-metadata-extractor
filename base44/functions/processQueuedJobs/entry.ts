import { createClientFromRequest } from 'npm:@base44/sdk@0.8.21';

// ── DURABLE QUEUE WORKER ────────────────────────────────────
// Primary continuation: self-invoke chain after each batch/job.
// Backup: 5-min scheduled automation catches anything that falls through.
//
// Flow:  scheduler OR kickoff → claim job → process batches → release lock
//        → self-invoke if (a) current job has remaining rows,
//          (b) another queued/running job exists, or (c) transient error.
//
// Safety:
//   - Atomic lock claim with verify-after-write (optimistic concurrency).
//   - Re-checks job status before every batch (respects pause/cancel).
//   - Self-invoke depth guard: max 200 chained invocations per request origin.
//   - Lock auto-expires after 5 min (scheduler recovers stale locks).

const MAX_WALL_MS = 18_000;          // 18s — well under 25s CPU limit
const LOCK_DURATION_MS = 5 * 60_000; // 5 min — matches scheduler interval
const STALE_RUNNING_MS = 10 * 60_000;// 10 min — recover stuck "running"
const MAX_CHAIN_DEPTH = 200;         // prevent runaway self-invoke loops

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

Deno.serve(async (req) => {
    const startTime = Date.now();

    try {
        const base44 = createClientFromRequest(req);
        const sr = base44.asServiceRole;
        const body = await req.json().catch(() => ({}));
        const chainDepth = (body._chain_depth || 0);

        // ── STEP 0: Recover stale jobs ──
        try {
            const runningJobs = await sr.entities.Job.filter({ status: 'running' }, 'updated_date', 10);
            const now = new Date();
            for (const rj of runningJobs) {
                const updatedAt = new Date(rj.updated_date);
                const staleDuration = now.getTime() - updatedAt.getTime();
                if (staleDuration > STALE_RUNNING_MS) {
                    console.log(`[queue] Recovering stale job ${rj.id} (stale ${Math.round(staleDuration / 1000)}s)`);
                    await sr.entities.Job.update(rj.id, {
                        status: 'queued',
                        processing_lock_token: null,
                        processing_lock_expires_at: null,
                        error_message: `Auto-requeued: no progress for ${Math.round(staleDuration / 60000)} min`,
                    });
                }
            }
        } catch (recoveryErr) {
            console.error('[queue] Recovery error (non-fatal):', recoveryErr.message);
        }

        // ── STEP 1: Find a queued job to process ──
        const queuedJobs = await sr.entities.Job.filter({ status: 'queued' }, 'created_date', 5);

        // Also check for running jobs that still have pending rows (e.g. after resume).
        // This ensures the chain doesn't stop just because status flipped to "running".
        let runningWithPending = [];
        if (!queuedJobs.length) {
            const rJobs = await sr.entities.Job.filter({ status: 'running' }, 'updated_date', 5);
            for (const rj of rJobs) {
                // Only consider unlocked running jobs (lock released after prior batch)
                if (rj.processing_lock_token) {
                    const lockExp = new Date(rj.processing_lock_expires_at || 0);
                    if (lockExp > new Date()) continue; // still locked by another worker
                }
                const pending = await sr.entities.JobRow.filter({ job_id: rj.id, status: 'pending' }, 'row_index', 1, 0);
                if (pending.length > 0) runningWithPending.push(rj);
                if (runningWithPending.length >= 1) break; // only need one
            }
        }

        const candidates = [...queuedJobs, ...runningWithPending];
        if (!candidates.length) {
            return Response.json({ message: 'No work found.' });
        }

        // ── Claim a job with atomic lock ──
        let claimedJob = null;
        const lockToken = crypto.randomUUID();
        const lockExpires = new Date(Date.now() + LOCK_DURATION_MS).toISOString();

        for (const candidate of candidates) {
            if (candidate.processing_lock_token && candidate.processing_lock_expires_at) {
                const lockExp = new Date(candidate.processing_lock_expires_at);
                if (lockExp > new Date()) {
                    console.log(`[queue] Job ${candidate.id} locked until ${candidate.processing_lock_expires_at} — skip.`);
                    continue;
                }
            }
            try {
                await sr.entities.Job.update(candidate.id, {
                    processing_lock_token: lockToken,
                    processing_lock_expires_at: lockExpires,
                });
                const [verified] = await sr.entities.Job.filter({ id: candidate.id });
                if (verified?.processing_lock_token === lockToken &&
                    (verified.status === 'queued' || verified.status === 'running')) {
                    claimedJob = verified;
                    console.log(`[queue] Claimed job ${candidate.id} (${verified.status}) depth=${chainDepth}`);
                    break;
                }
            } catch (claimErr) {
                console.error(`[queue] Claim failed ${candidate.id}:`, claimErr.message);
            }
        }

        if (!claimedJob) {
            return Response.json({ message: 'No claimable jobs (all locked).' });
        }

        const job_id = claimedJob.id;

        // ── STEP 2: Process batches within time limit ──
        let batchesRun = 0;
        let lastResult = null;
        let shouldContinue = false; // whether to self-invoke after this invocation

        while (Date.now() - startTime < MAX_WALL_MS) {
            const [freshJob] = await sr.entities.Job.filter({ id: job_id });
            if (!freshJob) { console.log(`[queue] Job ${job_id} gone.`); break; }
            if (['done', 'error', 'cancelled', 'paused'].includes(freshJob.status)) {
                console.log(`[queue] Job ${job_id} → ${freshJob.status}`);
                lastResult = { status: freshJob.status };
                break;
            }

            // Refresh lock expiry so it doesn't go stale mid-processing
            try {
                await sr.entities.Job.update(job_id, {
                    processing_lock_expires_at: new Date(Date.now() + LOCK_DURATION_MS).toISOString(),
                });
            } catch (_) { /* non-fatal */ }

            const pendingRows = await sr.entities.JobRow.filter(
                { job_id, status: 'pending' }, 'row_index', 1, 0
            );
            if (!pendingRows.length) {
                const processingRows = await sr.entities.JobRow.filter(
                    { job_id, status: 'processing' }, 'row_index', 1, 0
                );
                if (!processingRows.length) {
                    await sr.entities.Job.update(job_id, {
                        status: 'done',
                        processed_rows: freshJob.total_rows || freshJob.processed_rows,
                        processing_lock_token: null,
                        processing_lock_expires_at: null,
                    });
                    console.log(`[queue] Job ${job_id} complete.`);
                    lastResult = { status: 'done' };
                } else {
                    lastResult = { status: 'waiting_for_processing_rows' };
                    shouldContinue = true;
                }
                break;
            }

            let batchResp;
            try {
                batchResp = await base44.functions.invoke('jobProcessor', {
                    action: 'process',
                    job_id,
                    _service_call: true,
                });
            } catch (invokeErr) {
                console.error(`[queue] Batch ${batchesRun + 1} error:`, invokeErr.message);
                const [afterErr] = await sr.entities.Job.filter({ id: job_id });
                if (afterErr?.status === 'error') {
                    lastResult = { status: 'error', error: invokeErr.message };
                    break;
                }
                lastResult = { error: invokeErr.message };
                shouldContinue = true; // retry via next invocation
                break;
            }

            batchesRun++;
            const remaining = batchResp?.remaining ?? batchResp?.data?.remaining;
            lastResult = batchResp?.data || batchResp;
            console.log(`[queue] Batch ${batchesRun} done. remaining=${remaining}`);

            if (remaining === 0 || remaining === undefined) break;

            shouldContinue = true;

            if (Date.now() - startTime + 2000 >= MAX_WALL_MS) {
                console.log(`[queue] Time limit — will continue via self-invoke.`);
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
        } catch (_) {}

        const elapsed = Math.round((Date.now() - startTime) / 1000);
        console.log(`[queue] Done. job=${job_id} batches=${batchesRun} elapsed=${elapsed}s depth=${chainDepth}`);

        // ── STEP 4: Self-invoke for continuous processing ──
        // Check: more work on this job, OR other queued/running jobs with pending rows.
        if (!shouldContinue) {
            try {
                const moreQueued = await sr.entities.Job.filter({ status: 'queued' }, 'created_date', 1);
                if (moreQueued.length > 0) shouldContinue = true;
            } catch (_) {}
        }
        if (!shouldContinue) {
            // Check if there are running jobs with pending rows (e.g., multi-job queue)
            try {
                const runJobs = await sr.entities.Job.filter({ status: 'running' }, 'updated_date', 3);
                for (const rj of runJobs) {
                    if (rj.processing_lock_token) continue; // still locked
                    const p = await sr.entities.JobRow.filter({ job_id: rj.id, status: 'pending' }, 'row_index', 1, 0);
                    if (p.length) { shouldContinue = true; break; }
                }
            } catch (_) {}
        }

        if (shouldContinue && chainDepth < MAX_CHAIN_DEPTH) {
            try {
                // Fire-and-forget — if this fails, the 5-min scheduler recovers.
                base44.functions.invoke('processQueuedJobs', {
                    _self_invoke: true,
                    _chain_depth: chainDepth + 1,
                }).catch((err) => {
                    console.warn(`[queue] Self-invoke failed (scheduler backup): ${err.message}`);
                });
                console.log(`[queue] Self-invoked (depth ${chainDepth + 1}).`);
            } catch (selfErr) {
                console.warn(`[queue] Self-invoke failed (scheduler backup): ${selfErr.message}`);
            }
        } else if (shouldContinue) {
            console.warn(`[queue] Chain depth ${chainDepth} reached limit — stopping chain. Scheduler will resume.`);
        }

        return Response.json({
            job_id,
            batches_run: batchesRun,
            elapsed_seconds: elapsed,
            chain_depth: chainDepth,
            last_result: lastResult,
            self_invoked: shouldContinue && chainDepth < MAX_CHAIN_DEPTH,
        });

    } catch (error) {
        console.error('[queue] Fatal error:', error.message);
        return Response.json({ error: error.message }, { status: 500 });
    }
});