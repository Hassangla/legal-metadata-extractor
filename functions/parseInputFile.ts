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

        // ── Step 1: Download ────────────────────────────────────
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

        // ── Step 3: Read active spec to find expected columns ───
        // The spec defines what columns the input should have.
        // We use this to auto-detect which sheet contains the real data.
        let expectedColumns = [];
        try {
            const specs = await base44.entities.Spec.filter({ is_active: true });
            if (specs.length > 0) {
                const specText = specs[0].current_text || '';

                // Pattern 1: pipe-separated "columns: Owner | Economy | Legal basis"
                const pipeMatch = specText.match(/columns?\s*[:]\s*([^\n]*\|[^\n]*)/i);
                if (pipeMatch) {
                    expectedColumns = pipeMatch[1]
                        .split('|')
                        .map(c => c.trim().toLowerCase())
                        .filter(c => c.length > 0 && c.length < 50);
                }

                // Pattern 2: bullet list under "Input" heading
                // - Owner
                // - Economy
                // - Legal basis
                if (expectedColumns.length === 0) {
                    const inputSection = specText.match(/input[^\n]*\n((?:\s*[-*]\s+[^\n]+\n?)+)/i);
                    if (inputSection) {
                        expectedColumns = inputSection[1]
                            .split('\n')
                            .map(line => line.replace(/^\s*[-*]\s+/, '').trim().toLowerCase())
                            .filter(c => c.length > 0 && c.length < 50);
                    }
                }
            }
        } catch (_) {
            // Spec not available — will fall back to heuristics
        }

        // ── Step 4: Score each sheet → pick best match ──────────
        function normalizeHeader(h) {
            return (h || '').toLowerCase().trim().replace(/[_\s]+/g, ' ');
        }

        function scoreSheet(headers) {
            if (expectedColumns.length === 0) return 0;
            const normalized = headers.map(normalizeHeader);
            return expectedColumns.filter(ec =>
                normalized.some(h => h === ec || h === ec.replace(/\s+/g, '_'))
            ).length;
        }

        let bestSheetName = workbook.SheetNames[0];
        let bestScore = 0;
        const sheetSummaries = [];

        for (const name of workbook.SheetNames) {
            const ws = workbook.Sheets[name];
            if (!ws) {
                sheetSummaries.push({ name, rows: 0, columns: [], score: 0 });
                continue;
            }

            let sheetData;
            try {
                sheetData = XLSX.utils.sheet_to_json(ws, { defval: '' });
            } catch (_) {
                sheetSummaries.push({ name, rows: 0, columns: [], score: 0 });
                continue;
            }

            const headers = sheetData.length > 0 ? Object.keys(sheetData[0]) : [];
            const nonEmpty = sheetData.filter(row =>
                Object.values(row).some(v => v !== '' && v !== null && v !== undefined)
            );

            const score = scoreSheet(headers);
            sheetSummaries.push({
                name,
                rows: nonEmpty.length,
                columns: headers.map(h => h.trim()).filter(Boolean),
                score,
            });

            if (score > bestScore) {
                bestScore = score;
                bestSheetName = name;
            }
        }

        // If no sheet matched spec columns (score=0) and there are multiple sheets,
        // use density heuristic: pick the sheet with the highest data-fill ratio.
        if (bestScore === 0 && workbook.SheetNames.length > 1) {
            let bestDensity = -1;
            for (const summary of sheetSummaries) {
                if (summary.rows === 0 || summary.columns.length === 0) continue;
                const ws = workbook.Sheets[summary.name];
                const sheetData = XLSX.utils.sheet_to_json(ws, { defval: '' });
                let filled = 0;
                let total = 0;
                for (const row of sheetData) {
                    for (const v of Object.values(row)) {
                        total++;
                        if (v !== '' && v !== null && v !== undefined) filled++;
                    }
                }
                const density = total > 0 ? filled / total : 0;
                if (density > bestDensity) {
                    bestDensity = density;
                    bestSheetName = summary.name;
                }
            }
        }

        // ── Step 5: Read the selected sheet ─────────────────────
        const worksheet = workbook.Sheets[bestSheetName];

        if (!worksheet) {
            return Response.json({ error: `Sheet "${bestSheetName}" is empty or unreadable.` }, { status: 400 });
        }

        let data;
        try {
            data = XLSX.utils.sheet_to_json(worksheet, { defval: '' });
        } catch (e) {
            return Response.json({
                error: `Failed to convert sheet "${bestSheetName}" to rows: ${e.message}`
            }, { status: 400 });
        }

        if (!data || data.length === 0) {
            return Response.json({
                error: `Sheet "${bestSheetName}" has no data rows. Row 1 should have headers, row 2+ should have data.`
            }, { status: 400 });
        }

        const headers = Object.keys(data[0]);

        if (headers.length === 0) {
            return Response.json({
                error: 'No columns detected. Make sure row 1 contains column headers.'
            }, { status: 400 });
        }

        // ── Step 6: Pass through ALL columns as-is ──────────────
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

        const nonEmptyRows = rows.filter(row =>
            Object.values(row).some(v => v !== '' && v !== null && v !== undefined)
        );

        if (nonEmptyRows.length === 0) {
            return Response.json({
                error: `Sheet "${bestSheetName}" has no non-empty data rows.`
            }, { status: 400 });
        }

        return Response.json({
            success: true,
            rows: nonEmptyRows,
            total_rows: nonEmptyRows.length,
            columns: headers.map(h => h.trim()).filter(Boolean),
            selected_sheet: bestSheetName,
            all_sheets: sheetSummaries,
            matched_by: bestScore > 0 ? 'spec_columns' : (workbook.SheetNames.length > 1 ? 'density_heuristic' : 'only_sheet'),
        });

    } catch (error) {
        return Response.json({
            error: `Unexpected error: ${error.message}`
        }, { status: 500 });
    }
});