import { createClientFromRequest } from 'npm:@base44/sdk@0.8.21';
import * as XLSX from 'npm:xlsx@0.18.5';

// ── ECONOMY / CODE HEADER VARIANTS ──────────────────────────
const ECONOMY_HEADERS = new Set(['economy', 'economy_name', 'name', 'country', 'country_name']);
const CODE_HEADERS    = new Set(['economy_code', 'code', 'iso_code', 'iso3', 'country_code']);

// ── RFC 4180-compliant CSV parser ───────────────────────────
// Handles: BOM, quoted fields, commas/newlines inside quotes,
//          semicolon delimiter detection, whitespace trimming.

function parseCSVText(raw) {
    if (!raw || typeof raw !== 'string') return { error: 'Empty file' };

    // Strip UTF-8 BOM
    let text = raw;
    if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);
    text = text.trim();
    if (!text) return { error: 'Empty file' };

    // Tokenize respecting quoted fields (RFC 4180)
    function tokenize(input, delim) {
        const rows = [];
        let row = [];
        let i = 0;
        const len = input.length;

        while (i < len) {
            // Start of field
            if (input[i] === '"') {
                // Quoted field
                let field = '';
                i++; // skip opening quote
                while (i < len) {
                    if (input[i] === '"') {
                        if (i + 1 < len && input[i + 1] === '"') {
                            field += '"';
                            i += 2; // escaped quote
                        } else {
                            i++; // closing quote
                            break;
                        }
                    } else {
                        field += input[i];
                        i++;
                    }
                }
                row.push(field);
                // Consume delimiter or newline after closing quote
                if (i < len && input[i] === delim) {
                    i++;
                } else if (i < len && (input[i] === '\r' || input[i] === '\n')) {
                    if (input[i] === '\r' && i + 1 < len && input[i + 1] === '\n') i += 2;
                    else i++;
                    rows.push(row);
                    row = [];
                }
                // else: end of input, will flush below
            } else {
                // Unquoted field — read until delimiter or newline
                let field = '';
                while (i < len && input[i] !== delim && input[i] !== '\r' && input[i] !== '\n') {
                    field += input[i];
                    i++;
                }
                row.push(field);
                if (i < len && input[i] === delim) {
                    i++;
                } else if (i < len) {
                    if (input[i] === '\r' && i + 1 < len && input[i + 1] === '\n') i += 2;
                    else i++;
                    rows.push(row);
                    row = [];
                }
            }
        }
        // Flush last row
        if (row.length > 0) {
            // Skip if it's a single empty-string field at EOF
            if (!(row.length === 1 && row[0].trim() === '')) {
                rows.push(row);
            }
        }
        return rows;
    }

    // Try comma first, fall back to semicolon if headers don't match
    function detectDelimiter() {
        // Look at the first line only (up to the first unquoted newline)
        let firstLine = '';
        let inQ = false;
        for (let j = 0; j < text.length; j++) {
            if (text[j] === '"') inQ = !inQ;
            if (!inQ && (text[j] === '\n' || text[j] === '\r')) break;
            firstLine += text[j];
        }
        const commas = (firstLine.match(/,/g) || []).length;
        const semis  = (firstLine.match(/;/g) || []).length;
        // If semicolons present and more than commas, try semicolons first
        if (semis > 0 && semis >= commas) {
            const semiHeaders = firstLine.split(';').map(h => h.trim().replace(/^"|"$/g, '').toLowerCase());
            const hasSemiEcon = semiHeaders.some(h => ECONOMY_HEADERS.has(h));
            const hasSemiCode = semiHeaders.some(h => CODE_HEADERS.has(h));
            if (hasSemiEcon && hasSemiCode) return ';';
        }
        // Try comma
        if (commas > 0) {
            const commaHeaders = firstLine.split(',').map(h => h.trim().replace(/^"|"$/g, '').toLowerCase());
            const hasCommaEcon = commaHeaders.some(h => ECONOMY_HEADERS.has(h));
            const hasCommaCode = commaHeaders.some(h => CODE_HEADERS.has(h));
            if (hasCommaEcon && hasCommaCode) return ',';
        }
        // Fallback: whichever has more
        return semis > commas ? ';' : ',';
    }

    const delim = detectDelimiter();
    const allRows = tokenize(text, delim);
    if (allRows.length === 0) return { error: 'Empty file' };

    // Parse headers
    const headers = allRows[0].map(h => h.trim().toLowerCase());
    const economyIdx = headers.findIndex(h => ECONOMY_HEADERS.has(h));
    const codeIdx    = headers.findIndex(h => CODE_HEADERS.has(h));

    if (economyIdx === -1 || codeIdx === -1) {
        return {
            error: `Required headers not found. Expected one of [${[...ECONOMY_HEADERS].join(', ')}] and one of [${[...CODE_HEADERS].join(', ')}]. Found: [${headers.join(', ')}]`,
        };
    }

    const rows = [];
    for (let i = 1; i < allRows.length; i++) {
        const vals = allRows[i];
        const economy = (vals[economyIdx] || '').trim();
        const code    = (vals[codeIdx]    || '').trim();
        if (economy && code) {
            rows.push({ economy: economy.toLowerCase(), economy_code: code.toUpperCase() });
        }
    }
    return { rows };
}

// Excel parser for economy codes (uses shared header sets)
function parseExcel(buffer) {
    const workbook = XLSX.read(new Uint8Array(buffer), { type: 'array' });
    if (!workbook.SheetNames || workbook.SheetNames.length === 0) return null;

    for (const sheetName of workbook.SheetNames) {
        const ws = workbook.Sheets[sheetName];
        if (!ws) continue;

        const data = XLSX.utils.sheet_to_json(ws, { defval: '' });
        if (!data || data.length === 0) continue;

        const economyKey = Object.keys(data[0]).find(h => ECONOMY_HEADERS.has(h.toLowerCase().trim()));
        const codeKey    = Object.keys(data[0]).find(h => CODE_HEADERS.has(h.toLowerCase().trim()));

        if (!economyKey || !codeKey) continue;

        const rows = [];
        for (const row of data) {
            const economy = String(row[economyKey] || '').trim();
            const code = String(row[codeKey] || '').trim();
            if (economy && code) {
                rows.push({ economy: economy.toLowerCase(), economy_code: code.toUpperCase() });
            }
        }
        if (rows.length > 0) return rows;
    }
    return null;
}

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();

        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const { action, ...params } = await req.json();

        switch (action) {
            case 'list': {
                const codes = await base44.entities.EconomyCode.list();
                return Response.json({ codes });
            }

            // MERGE import: upserts instead of delete-all.
            case 'import': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required' }, { status: 403 });
                }

                const { data } = params;

                if (!data || !Array.isArray(data)) {
                    return Response.json({ error: 'Data array required' }, { status: 400 });
                }

                const existing = await base44.entities.EconomyCode.list();
                const existingMap = {};
                for (const ec of existing) {
                    existingMap[ec.economy.toLowerCase().trim()] = ec;
                }

                let imported = 0;
                let updated = 0;
                let skipped = 0;

                for (const item of data) {
                    if (!item.economy || !item.economy_code) continue;
                    const key = item.economy.toLowerCase().trim();
                    const code = item.economy_code.trim().toUpperCase();
                    const existingEntry = existingMap[key];

                    if (existingEntry) {
                        if (existingEntry.economy_code !== code) {
                            await base44.entities.EconomyCode.update(existingEntry.id, {
                                economy_code: code
                            });
                            updated++;
                        } else {
                            skipped++;
                        }
                    } else {
                        await base44.entities.EconomyCode.create({
                            economy: key,
                            economy_code: code
                        });
                        imported++;
                    }
                }

                return Response.json({ success: true, imported, updated, skipped });
            }

            // Import from file URL (CSV or Excel) — single entry point for all file types
            case 'importFromFile': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required' }, { status: 403 });
                }

                const { file_url, file_name } = params;
                if (!file_url) {
                    return Response.json({ error: 'file_url is required' }, { status: 400 });
                }

                let fileResponse;
                try {
                    fileResponse = await fetch(file_url);
                } catch (fetchErr) {
                    return Response.json({ error: `Failed to download uploaded file: ${fetchErr.message}` }, { status: 400 });
                }
                if (!fileResponse.ok) {
                    return Response.json({ error: `Failed to fetch uploaded file (HTTP ${fileResponse.status})` }, { status: 400 });
                }

                const fname = (file_name || '').toLowerCase();
                let rows;

                if (fname.endsWith('.xlsx') || fname.endsWith('.xls')) {
                    // ── Excel path: dedicated try/catch so parser failures → 400 ──
                    let buffer;
                    try {
                        buffer = await fileResponse.arrayBuffer();
                    } catch (readErr) {
                        return Response.json({ error: `Could not read file content: ${readErr.message}` }, { status: 400 });
                    }
                    if (!buffer || buffer.byteLength === 0) {
                        return Response.json({ error: 'Uploaded Excel file is empty (0 bytes).' }, { status: 400 });
                    }
                    try {
                        rows = parseExcel(buffer);
                    } catch (parseErr) {
                        // XLSX.read can throw for corrupt/password-protected/non-Excel files
                        const hint = parseErr.message || String(parseErr);
                        return Response.json({
                            error: `Invalid or unreadable Excel file. The workbook could not be parsed. Detail: ${hint.slice(0, 300)}`
                        }, { status: 400 });
                    }
                    if (rows === null) {
                        return Response.json({
                            error: `No usable worksheet data found. Expected at least one sheet with headers like: [${[...ECONOMY_HEADERS].join(', ')}] and [${[...CODE_HEADERS].join(', ')}].`
                        }, { status: 400 });
                    }
                } else {
                    // ── CSV / TSV / text path ──
                    let csvText;
                    try {
                        csvText = await fileResponse.text();
                    } catch (readErr) {
                        return Response.json({ error: `Could not read file content: ${readErr.message}` }, { status: 400 });
                    }
                    const result = parseCSVText(csvText);
                    if (result.error) {
                        return Response.json({ error: result.error }, { status: 400 });
                    }
                    rows = result.rows;
                }

                if (!rows || rows.length === 0) {
                    return Response.json({ error: 'File parsed successfully but contained no valid data rows.' }, { status: 400 });
                }

                // ── DEDUPLICATE & VALIDATE uploaded rows before any DB writes ──
                const deduped = [];
                const seenInFile = {}; // economy_key → { code, fileRow (1-based) }
                const rowErrors = [];

                for (let i = 0; i < rows.length; i++) {
                    const fileRow = i + 2; // row 1 = headers, data starts at row 2
                    const economy = (rows[i].economy || '').trim().toLowerCase();
                    const code = (rows[i].economy_code || '').trim().toUpperCase();

                    if (!economy) {
                        rowErrors.push({ row: fileRow, reason: 'Missing economy name' });
                        continue;
                    }
                    if (!code) {
                        rowErrors.push({ row: fileRow, reason: `Missing economy code for "${economy}"` });
                        continue;
                    }

                    const prev = seenInFile[economy];
                    if (prev) {
                        if (prev.code !== code) {
                            rowErrors.push({
                                row: fileRow,
                                reason: `Conflicting code for "${economy}": row ${prev.fileRow} has "${prev.code}", row ${fileRow} has "${code}"`
                            });
                        }
                        // Either way, skip the duplicate
                        continue;
                    }

                    seenInFile[economy] = { code, fileRow };
                    deduped.push({ economy, economy_code: code });
                }

                // If zero usable rows after validation, return structured error
                if (deduped.length === 0) {
                    const detail = rowErrors.length > 0
                        ? rowErrors.slice(0, 5).map(e => `Row ${e.row}: ${e.reason}`).join('; ')
                          + (rowErrors.length > 5 ? ` (and ${rowErrors.length - 5} more)` : '')
                        : 'File contained no usable data.';
                    return Response.json({ error: `No valid rows after validation. ${detail}` }, { status: 400 });
                }

                // ── MERGE: upsert against existing DB records ──
                const existing = await base44.entities.EconomyCode.list();
                const existingMap = {};
                for (const ec of existing) {
                    existingMap[ec.economy.toLowerCase().trim()] = ec;
                }

                let imported = 0;
                let updated = 0;
                let skipped = 0;
                const writeErrors = [];

                for (const item of deduped) {
                    const key = item.economy;
                    const code = item.economy_code;
                    const existingEntry = existingMap[key];

                    try {
                        if (existingEntry) {
                            if (existingEntry.economy_code !== code) {
                                await base44.entities.EconomyCode.update(existingEntry.id, { economy_code: code });
                                existingEntry.economy_code = code; // update in-memory to prevent stale reads
                                updated++;
                            } else {
                                skipped++;
                            }
                        } else {
                            const created = await base44.entities.EconomyCode.create({
                                economy: key,
                                economy_code: code
                            });
                            // Track in-memory so later rows referencing the same economy won't double-create
                            existingMap[key] = { id: created.id, economy: key, economy_code: code };
                            imported++;
                        }
                    } catch (writeErr) {
                        writeErrors.push({ economy: key, reason: writeErr.message || 'Write failed' });
                    }
                }

                const resp = { success: true, imported, updated, skipped, total: deduped.length };
                if (rowErrors.length > 0) {
                    resp.row_warnings = rowErrors.slice(0, 20);
                    resp.row_warning_count = rowErrors.length;
                }
                if (writeErrors.length > 0) {
                    resp.write_errors = writeErrors.slice(0, 20);
                    resp.write_error_count = writeErrors.length;
                }
                return Response.json(resp);
            }

            case 'update': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required' }, { status: 403 });
                }
                const { id, economy, economy_code } = params;
                if (!id) return Response.json({ error: 'id is required' }, { status: 400 });

                const updateData = {};
                if (economy !== undefined) updateData.economy = economy.toLowerCase().trim();
                if (economy_code !== undefined) updateData.economy_code = economy_code.trim().toUpperCase();

                await base44.entities.EconomyCode.update(id, updateData);
                return Response.json({ success: true });
            }

            case 'delete': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required' }, { status: 403 });
                }
                const { id } = params;
                if (!id) return Response.json({ error: 'id is required' }, { status: 400 });

                await base44.entities.EconomyCode.delete(id);
                return Response.json({ success: true });
            }

            case 'lookup': {
                // Strict exact match only (trim + case-insensitive). No fuzzy/prefix stripping.
                const { economy } = params;
                if (!economy) return Response.json({ error: 'Economy name required' }, { status: 400 });
                const codes = await base44.entities.EconomyCode.list();
                const needle = economy.toLowerCase().trim();
                const match = codes.find(c => c.economy.toLowerCase().trim() === needle);
                return Response.json({ found: !!match, economy_code: match?.economy_code || null });
            }

            case 'deleteAll': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required' }, { status: 403 });
                }
                const existing = await base44.entities.EconomyCode.list();
                for (const code of existing) {
                    await base44.entities.EconomyCode.delete(code.id);
                }
                return Response.json({ success: true, deleted: existing.length });
            }

            default:
                return Response.json({ error: 'Unknown action' }, { status: 400 });
        }
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});