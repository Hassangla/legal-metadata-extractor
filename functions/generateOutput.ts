import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';
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

        const rows = await base44.entities.JobRow.filter({ job_id });
        rows.sort((a, b) => a.row_index - b.row_index);

        // ── OUTPUT SHEET (13 columns) ──
        const outputData = rows.map(row => {
            const o = row.output_json || {};
            const input = row.input_data || {};
            return {
                'ID': '',
                'Economy_Code': o.Economy_Code || '',
                'Economy': o.Economy || input.Economy || '',
                'Language_Doc': o.Language_Doc || '',
                'Instrument_Full_Name_Original_Language': o.Instrument_Full_Name_Original_Language || '',
                'Instrument_Published_Name': o.Instrument_Published_Name || '',
                'Instrument_URL': o.Instrument_URL || '',
                'Enactment_Date': o.Enactment_Date || '',
                'Date of Entry in Force': o.Date_of_Entry_in_Force || o['Date of Entry in Force'] || o['Date_of_Entry_in_Force'] || '',
                'Repeal_Year': o.Repeal_Year || '',
                'Current Status': o.Current_Status || o['Current Status'] || o['Current_Status'] || '',
                'Public': o.Public || '',
                'Flag': o.Flag || '',
                'Extraction_Status': o.Extraction_Status || '',
            };
        });

        // ── EVIDENCE SHEET (30 columns) ──
        const evidenceData = rows.map(row => {
            const e = row.evidence_json || {};
            const o = row.output_json || {};
            const input = row.input_data || {};

            return {
                'Row_Index': e.Row_Index || row.row_index,
                'Economy': e.Economy || o.Economy || input.Economy || '',
                'Economy_Code': e.Economy_Code || o.Economy_Code || '',
                'Legal_basis_verbatim': e.Legal_basis_verbatim || input.Legal_basis || input['Legal basis'] || '',
                'Query_1': e.Query_1 || '',
                'Query_2': e.Query_2 || '',
                'Query_3': e.Query_3 || '',
                'URLs_Considered': e.URLs_Considered || '',
                'Selected_Source_URLs': e.Selected_Source_URLs || '',
                'Source_Tier': e.Source_Tier || e.Tier || '',
                'Public_Access': e.Public_Access || '',
                'Raw_Official_Title_As_Source': e.Raw_Official_Title_As_Source || '',
                'Normalized_Title_Used': e.Normalized_Title_Used || '',
                'Language_Justification': e.Language_Justification || '',
                'Instrument_URL_Support': e.Instrument_URL_Support || '',
                'Enactment_Support': e.Enactment_Support || '',
                'EntryIntoForce_Support': e.EntryIntoForce_Support || '',
                'Status_Support': e.Status_Support || '',
                'Missing_Conflict_Reason': e.Missing_Conflict_Reason || e['Missing/Conflict_Reason'] || '',
                'Normalization_Notes': e.Normalization_Notes || '',
                // Final_* fields — mirror from Output for evidence-to-output sync
                'Final_Language_Doc': o.Language_Doc || '',
                'Final_Instrument_Full_Name_Original_Language': o.Instrument_Full_Name_Original_Language || '',
                'Final_Instrument_Published_Name': o.Instrument_Published_Name || '',
                'Final_Instrument_URL': o.Instrument_URL || '',
                'Final_Enactment_Date': o.Enactment_Date || '',
                'Final_Date_of_Entry_in_Force': o.Date_of_Entry_in_Force || o['Date of Entry in Force'] || '',
                'Final_Repeal_Year': o.Repeal_Year || '',
                'Final_Current_Status': o.Current_Status || o['Current Status'] || '',
                'Final_Public': o.Public || '',
                'Final_Flag': o.Flag || '',
            };
        });

        const wb = XLSX.utils.book_new();
        const wsOutput = XLSX.utils.json_to_sheet(outputData);
        XLSX.utils.book_append_sheet(wb, wsOutput, 'Output');
        const wsEvidence = XLSX.utils.json_to_sheet(evidenceData);
        XLSX.utils.book_append_sheet(wb, wsEvidence, 'Evidence');

        const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
        const base64Data = uint8ArrayToBase64(new Uint8Array(buffer));

        // Generate Eastern Time timestamp using reliable Intl.DateTimeFormat
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

        return Response.json({
            success: true,
            filename,
            data: base64Data,
            mimeType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        });
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});