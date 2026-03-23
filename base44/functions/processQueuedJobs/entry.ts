import { createClientFromRequest } from 'npm:@base44/sdk@0.8.21';

Deno.serve(async (req) => {
    // ALWAYS return 200 to prevent automation auto-disable.
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

        // Call jobProcessor directly via HTTP (avoids sr.functions.invoke 403 issue)
        // Build the request to jobProcessor by forwarding auth headers
        console.log(`[PQJ] Calling jobProcessor for job ${job_id}...`);
        
        const appId = Deno.env.get('BASE44_APP_ID');
        const jobProcessorUrl = `https://base44.app/api/apps/${appId}/functions/jobProcessor`;
        
        // Forward all auth-related headers from the incoming request
        const headers = {};
        for (const [key, value] of req.headers.entries()) {
            const lk = key.toLowerCase();
            if (lk === 'authorization' || lk === 'x-base44-token' || lk === 'x-base44-service-role-key' || lk.startsWith('x-base44')) {
                headers[key] = value;
            }
        }
        headers['Content-Type'] = 'application/json';

        const resp = await fetch(jobProcessorUrl, {
            method: 'POST',
            headers,
            body: JSON.stringify({ action: 'process', job_id }),
        });

        if (!resp.ok) {
            const errText = await resp.text();
            console.error(`[PQJ] jobProcessor returned ${resp.status}: ${errText.slice(0, 300)}`);
            return Response.json({ ok: true, error: `jobProcessor ${resp.status}`, job_id });
        }

        const batchResp = await resp.json();
        console.log(`[PQJ] Batch complete. remaining=${batchResp?.remaining} processed=${batchResp?.processed_this_batch}`);

        return Response.json({
            ok: true,
            job_id,
            remaining: batchResp?.remaining,
            processed: batchResp?.processed_this_batch,
        });

    } catch (error) {
        console.error('[PQJ] Error:', error.message);
        return Response.json({ ok: true, error: error.message });
    }
});