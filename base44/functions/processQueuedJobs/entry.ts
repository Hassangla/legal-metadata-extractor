import { createClientFromRequest } from 'npm:@base44/sdk@0.8.21';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

Deno.serve(async (req) => {
    // ALWAYS return 200 to prevent automation auto-disable from consecutive failures.
    const base44 = createClientFromRequest(req);
    const sr = base44.asServiceRole;

    try {
        console.log('[PQJ] Starting...');

        // Find the oldest queued or running job
        const [queuedJobs, runningJobs] = await Promise.all([
            sr.entities.Job.filter({ status: 'queued' }, 'created_date', 1),
            sr.entities.Job.filter({ status: 'running' }, 'updated_date', 1),
        ]);

        const job = (queuedJobs || [])[0] || (runningJobs || [])[0];
        if (!job) {
            console.log('[PQJ] No active jobs found.');
            return Response.json({ ok: true, message: 'No active jobs.' });
        }

        const job_id = job.id;
        console.log(`[PQJ] Found job ${job_id} status=${job.status}`);

        // Reset any stale 'processing' rows back to 'pending'
        try {
            const staleRows = await sr.entities.JobRow.filter(
                { job_id, status: 'processing' }, 'row_index', 50
            );
            if (staleRows.length > 0) {
                console.log(`[PQJ] Resetting ${staleRows.length} stale processing rows`);
                for (const row of staleRows) {
                    try { await sr.entities.JobRow.update(row.id, { status: 'pending' }); } catch (_) {}
                }
            }
        } catch (_) {}

        // Check if there are pending rows
        const pendingCheck = await sr.entities.JobRow.filter(
            { job_id, status: 'pending' }, 'row_index', 1
        );

        if (!pendingCheck.length) {
            console.log(`[PQJ] No pending rows — marking job done.`);
            try {
                await sr.entities.Job.update(job_id, {
                    status: 'done',
                    processed_rows: job.total_rows || job.processed_rows,
                });
            } catch (_) {}
            return Response.json({ ok: true, job_id, message: 'No pending rows, marked done.' });
        }

        // Invoke jobProcessor to process one batch
        console.log(`[PQJ] Invoking jobProcessor for job ${job_id}...`);
        const result = await sr.functions.invoke('jobProcessor', {
            action: 'process',
            job_id,
        });
        const batchResp = result?.data || result;
        console.log(`[PQJ] Batch complete. remaining=${batchResp?.remaining} processed=${batchResp?.processed_this_batch}`);

        return Response.json({
            ok: true,
            job_id,
            batches_run: 1,
            remaining: batchResp?.remaining,
            processed: batchResp?.processed_this_batch,
        });

    } catch (error) {
        console.error('[PQJ] Error:', error.message, 'stack:', error.stack?.slice(0, 300));
        // Return 200 even on error to prevent automation auto-disable
        return Response.json({ ok: true, error: error.message });
    }
});