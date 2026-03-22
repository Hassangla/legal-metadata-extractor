import { createClientFromRequest } from 'npm:@base44/sdk@0.8.21';

// ════════════════════════════════════════════════════════════════════════
// ⚠️  NON-RUNTIME TEST ENDPOINT — NOT USED BY JOB PROCESSING
//
// This file is a standalone diagnostic/test endpoint. It is NOT imported
// by jobProcessor.js or any other runtime code. The authoritative
// implementations of all URL extraction, provenance, and search logic
// live exclusively in:
//
//   → functions/jobProcessor.js  (SINGLE SOURCE OF TRUTH)
//
// Previously this file contained duplicated copies of:
//   - extractToolUrlsFromResponse
//   - isNoSearchToolError
//   - urlInList
//   - isSafeHttpUrl
//
// Those duplicates were removed because they had already drifted from
// the live runtime versions (missing Kimi tool_call handling, Anthropic
// citation extraction, top-level annotations, etc.) and could mislead
// future developers into thinking the stale logic was correct.
//
// If you need to test URL extraction or provenance logic, write tests
// that call jobProcessor directly via base44.functions.invoke, or copy
// the exact current helpers from jobProcessor.js and clearly date-stamp
// the copy.
// ════════════════════════════════════════════════════════════════════════

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        if (!user) return Response.json({ error: 'Unauthorized' }, { status: 401 });

        return Response.json({
            success: true,
            message: 'testSearchVerification has been retired. Duplicated helper logic was removed to prevent drift from the authoritative runtime in jobProcessor.js.',
            authoritative_file: 'functions/jobProcessor.js',
            removed_duplicates: [
                'extractToolUrlsFromResponse',
                'isNoSearchToolError',
                'urlInList',
                'isSafeHttpUrl',
            ],
            reason: 'Stale copies had already drifted (missing Kimi tool_calls, Anthropic citations, top-level annotations). Keeping them risked reintroducing unsafe URL promotion logic.',
        });
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});