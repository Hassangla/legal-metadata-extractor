import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';
import * as XLSX from 'npm:xlsx@0.18.5';

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();

        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        let body;
        try {
            body = await req.json();
        } catch (_) {
            return Response.json({ error: 'Invalid request body. Expected JSON with file_url.' }, { status: 400 });
        }

        const { file_url } = body;

        if (!file_url) {
            return Response.json({ error: 'file_url is required but was empty or missing.' }, { status: 400 });
        }

        // ── Step 1: Download the uploaded file ──────────────────
        let response;
        try {
            response = await fetch(file_url);
        } catch (fetchError) {
            return Response.json({
                error: `Could not download file. Network error: ${fetchError.message}`
            }, { status: 500 });
        }

        if (!response.ok) {
            return Response.json({
                error: `Could not download file (HTTP ${response.status}). The upload URL may have expired — try uploading again.`
            }, { status: 500 });
        }

        const arrayBuffer = await response.arrayBuffer();

        if (arrayBuffer.byteLength === 0) {
            return Response.json({ error: 'The uploaded file is empty (0 bytes).' }, { status: 400 });
        }

        // ── Step 2: Parse Excel ─────────────────────────────────
        let workbook;
        try {
            workbook = XLSX.read(new Uint8Array(arrayBuffer), { type: 'array' });
        } catch (parseError) {
            return Response.json({
                error: `Could not parse file as Excel. Make sure it is a valid .xlsx or .xls. Detail: ${parseError.message}`
            }, { status: 400 });
        }

        if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
            return Response.json({ error: 'Excel file contains no sheets.' }, { status: 400 });
        }

        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];

        if (!worksheet) {
            return Response.json({ error: `Sheet "${sheetName}" is empty or unreadable.` }, { status: 400 });
        }

        // ── Step 3: Convert to row objects ──────────────────────
        let data;
        try {
            data = XLSX.utils.sheet_to_json(worksheet, { defval: '' });
        } catch (e) {
            return Response.json({
                error: `Failed to convert sheet to rows: ${e.message}`
            }, { status: 400 });
        }

        if (!data || data.length === 0) {
            return Response.json({
                error: 'The first sheet has no data rows. Row 1 should have headers, row 2+ should have data.'
            }, { status: 400 });
        }

        const headers = Object.keys(data[0]);

        if (headers.length === 0) {
            return Response.json({
                error: 'No columns detected. Make sure row 1 contains column headers.'
            }, { status: 400 });
        }

        // ── Step 4: Pass through ALL columns as-is ──────────────
        // No hardcoded column validation. No renaming. No dropping.
        // The Specification (system prompt) defines what columns mean.
        // The LLM interprets them at runtime.
        const rows = data.map(row => {
            const cleaned = {};
            for (const [key, value] of Object.entries(row)) {
                const trimmedKey = key.trim();
                if (trimmedKey) {
                    cleaned[trimmedKey] = typeof value === 'string' ? value.trim() : value;
                }
            }
            return cleaned;
        });

        // Filter out completely empty rows (all values blank)
        const nonEmptyRows = rows.filter(row =>
            Object.values(row).some(v => v !== '' && v !== null && v !== undefined)
        );

        if (nonEmptyRows.length === 0) {
            return Response.json({
                error: 'All data rows are empty. Make sure your data starts in row 2.'
            }, { status: 400 });
        }

        return Response.json({
            success: true,
            rows: nonEmptyRows,
            total_rows: nonEmptyRows.length,
            columns: headers.map(h => h.trim()).filter(Boolean)
        });

    } catch (error) {
        return Response.json({
            error: `Unexpected error: ${error.message}`
        }, { status: 500 });
    }
});