import { createClientFromRequest } from 'npm:@base44/sdk@0.8.20';
import * as XLSX from 'npm:xlsx@0.18.5';

function uint8ArrayToBase64(uint8Array) {
    const CHUNK_SIZE = 32768;
    let binaryString = '';
    for (let i = 0; i < uint8Array.length; i += CHUNK_SIZE) {
        const chunk = uint8Array.subarray(i, Math.min(i + CHUNK_SIZE, uint8Array.length));
        for (let j = 0; j < chunk.length; j++) {
            binaryString += String.fromCharCode(chunk[j]);
        }
    }
    return btoa(binaryString);
}

// Excel hard limit: 32,767 chars per cell
const xlCell = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return s.length > 32767 ? s.slice(0, 32767) : s;
};

const OUTPUT_HEADERS = [
    'ID', 'Economy_Code', 'Economy', 'Language_Doc',
    'Instrument_Full_Name_Original_Language', 'Instrument_Published_Name',
    'Instrument_URL', 'Enactment_Date', 'Date of Entry in Force',
    'Repeal_Year', 'Current Status', 'Public', 'Flag',
];

const EVIDENCE_HEADERS = [
    'Row_Index', 'Economy', 'Economy_Code', 'Legal_basis_verbatim',
    'Query_1', 'Query_2', 'Query_3',
    'URLs_Considered', 'Selected_Source_URLs', 'Source_Tier', 'Public_Access',
    'Raw_Official_Title_As_Source', 'Normalized_Title_Used',
    'Language_Justification', 'Instrument_URL_Support',
    'Enactment_Support', 'EntryIntoForce_Support', 'Status_Support',
    'Missing/Conflict_Reason', 'Normalization_Notes',
    'Final_Language_Doc', 'Final_Instrument_Full_Name_Original_Language',
    'Final_Instrument_Published_Name', 'Final_Instrument_URL',
    'Final_Enactment_Date', 'Final_Date_of_Entry_in_Force',
    'Final_Repeal_Year', 'Final_Current_Status', 'Final_Public', 'Final_Flag',
];

// Handles values that may be double- or triple-encoded JSON strings
function safeJson(val) {
    if (!val) return {};
    let result = val;
    for (let i = 0; i < 3 && typeof result === 'string'; i++) {
        try { result = JSON.parse(result); } catch { return {}; }
    }
    return (result && typeof result === 'object' && !Array.isArray(result)) ? result : {};
}

function rowToOutputAoaRow(row) {
    const e = safeJson(row.evidence_json);
    const o = safeJson(row.output_json);
    const input = safeJson(row.input_data);
    return [
        '',
        xlCell(e.Economy_Code || o.Economy_Code),
        xlCell(e.Economy || o.Economy || input.Economy),
        xlCell(e.Final_Language_Doc || o.Language_Doc),
        xlCell(e.Final_Instrument_Full_Name_Original_Language || o.Instrument_Full_Name_Original_Language),
        xlCell(e.Final_Instrument_Published_Name || o.Instrument_Published_Name),
        xlCell(e.Final_Instrument_URL || o.Instrument_URL),
        xlCell(e.Final_Enactment_Date || o.Enactment_Date),
        xlCell(e.Final_Date_of_Entry_in_Force || o.Date_of_Entry_in_Force),
        xlCell(e.Final_Repeal_Year || o.Repeal_Year),
        xlCell(e.Final_Current_Status || o.Current_Status),
        xlCell(e.Final_Public || o.Public),
        xlCell(e.Final_Flag || o.Flag),
    ];
}

function rowToEvidenceAoaRow(row) {
    const e = safeJson(row.evidence_json);
    const input = safeJson(row.input_data);
    return [
        e.Row_Index || row.row_index,
        xlCell(e.Economy || input.Economy),
        xlCell(e.Economy_Code),
        xlCell(e.Legal_basis_verbatim || input.Legal_basis || input['Legal basis']),
        xlCell(e.Query_1),
        xlCell(e.Query_2),
        xlCell(e.Query_3),
        xlCell(e.URLs_Considered),
        xlCell(e.Selected_Source_URLs),
        xlCell(e.Source_Tier || e.Tier),
        xlCell(e.Public_Access),
        xlCell(e.Raw_Official_Title_As_Source),
        xlCell(e.Normalized_Title_Used),
        xlCell(e.Language_Justification),
        xlCell(e.Instrument_URL_Support),
        xlCell(e.Enactment_Support),
        xlCell(e.EntryIntoForce_Support),
        xlCell(e.Status_Support),
        xlCell(e.Missing_Conflict_Reason || e['Missing/Conflict_Reason']),
        xlCell(e.Normalization_Notes),
        xlCell(e.Final_Language_Doc),
        xlCell(e.Final_Instrument_Full_Name_Original_Language),
        xlCell(e.Final_Instrument_Published_Name),
        xlCell(e.Final_Instrument_URL),
        xlCell(e.Final_Enactment_Date),
        xlCell(e.Final_Date_of_Entry_in_Force),
        xlCell(e.Final_Repeal_Year),
        xlCell(e.Final_Current_Status),
        xlCell(e.Final_Public),
        xlCell(e.Final_Flag),
    ];
}

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        if (!user) return Response.json({ error: 'Unauthorized' }, { status: 401 });

        const { job_id } = await req.json();
        if (!job_id) return Response.json({ error: 'job_id is required' }, { status: 400 });

        const jobs = await base44.entities.Job.filter({ id: job_id });
        if (jobs.length === 0) return Response.json({ error: 'Job not found' }, { status: 404 });
        const job = jobs[0];

        // Fetch rows in pages of 200 to avoid memory limits
        const PAGE_SIZE = 200;
        const outputAoa = [OUTPUT_HEADERS];
        const evidenceAoa = [EVIDENCE_HEADERS];

        let skip = 0;
        let debugFirstRow = null;
        let totalFetched = 0;
        let nonEmptyOutputRows = 0;
        while (true) {
            console.log(`[generateOutput] Fetching page: job_id=${job_id}, skip=${skip}, PAGE_SIZE=${PAGE_SIZE}`);
            let page = await base44.entities.JobRow.filter(
                { job_id },
                'row_index',
                PAGE_SIZE,
                skip
            );
            console.log(`[generateOutput] Raw page type: ${typeof page}, isArray: ${Array.isArray(page)}, length: ${Array.isArray(page) ? page.length : 'N/A'}`);
            if (page && typeof page === 'object' && !Array.isArray(page)) {
                console.log(`[generateOutput] Page is object, keys: ${Object.keys(page).slice(0, 10).join(', ')}`);
            }
            if (typeof page === 'string') { try { page = JSON.parse(page); } catch { page = []; } }
            if (!Array.isArray(page) || !page.length) break;
            totalFetched += page.length;

            for (const row of page) {
                if (!debugFirstRow) {
                    debugFirstRow = {
                        keys: Object.keys(row),
                        evidence_json_type: typeof row.evidence_json,
                        output_json_type: typeof row.output_json,
                        input_data_type: typeof row.input_data,
                        evidence_json_preview: JSON.stringify(row.evidence_json).slice(0, 300),
                        output_json_preview: JSON.stringify(row.output_json).slice(0, 300),
                    };
                }
                const outputRow = rowToOutputAoaRow(row);
                const hasContent = outputRow.some((cell, i) => i > 0 && cell !== '');
                if (hasContent) nonEmptyOutputRows++;
                outputAoa.push(outputRow);
                evidenceAoa.push(rowToEvidenceAoaRow(row));
            }

            if (page.length < PAGE_SIZE) break;
            skip += PAGE_SIZE;
        }
        console.log(`[generateOutput] Fetched ${totalFetched} rows, ${nonEmptyOutputRows} with non-empty output, ${outputAoa.length - 1} total data rows`);
        if (debugFirstRow) console.log(`[generateOutput] First row debug:`, JSON.stringify(debugFirstRow).slice(0, 500));

        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(outputAoa), 'Output');
        XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(evidenceAoa), 'Evidence');

        const arrayBuf = XLSX.write(wb, { type: 'array', bookType: 'xlsx' });
        const base64Data = uint8ArrayToBase64(new Uint8Array(arrayBuf));

        // Generate Eastern Time timestamp
        const now = new Date();
        const fmt = new Intl.DateTimeFormat('en-US', {
            timeZone: 'America/New_York',
            year: 'numeric', month: '2-digit', day: '2-digit',
            hour: '2-digit', minute: '2-digit', second: '2-digit',
            hour12: false,
        });
        const parts = {};
        for (const { type, value } of fmt.formatToParts(now)) {
            parts[type] = value;
        }
        const timestamp = `${parts.year}-${parts.month}-${parts.day}T${parts.hour}-${parts.minute}-${parts.second}`;
        const filename = `legal_metadata_output_${timestamp}.xlsx`;

        const base64Len = base64Data.length;

        // TEMP DEBUG: return only debug info
        return Response.json({
            _debug: {
                totalDataRows: outputAoa.length - 1,
                totalFetched,
                nonEmptyOutputRows,
                firstRow: debugFirstRow,
                base64Size: base64Len,
                sampleOutputRow: outputAoa.length > 1 ? outputAoa[1] : null,
                sampleEvidenceRow: evidenceAoa.length > 1 ? evidenceAoa[1]?.slice(0, 5) : null,
            },
        });
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});