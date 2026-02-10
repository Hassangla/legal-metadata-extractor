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

        // ── OUTPUT SHEET — exactly 13 columns, exact order via aoa_to_sheet ──
        const OUTPUT_HEADERS = [
            'ID', 'Economy_Code', 'Economy', 'Language_Doc',
            'Instrument_Full_Name_Original_Language', 'Instrument_Published_Name',
            'Instrument_URL', 'Enactment_Date', 'Date of Entry in Force',
            'Repeal_Year', 'Current Status', 'Public', 'Flag',
        ];

        const outputAoa = [OUTPUT_HEADERS];
        for (const row of rows) {
            const e = row.evidence_json || {};
            const o = row.output_json || {};
            const input = row.input_data || {};
            const hasFinals = e.Final_Flag !== undefined;
            outputAoa.push([
                '',  // ID — blank, do not edit
                e.Economy_Code || o.Economy_Code || '',
                e.Economy || o.Economy || input.Economy || '',
                hasFinals ? (e.Final_Language_Doc || '') : (o.Language_Doc || ''),
                hasFinals ? (e.Final_Instrument_Full_Name_Original_Language || '') : (o.Instrument_Full_Name_Original_Language || ''),
                hasFinals ? (e.Final_Instrument_Published_Name || '') : (o.Instrument_Published_Name || ''),
                hasFinals ? (e.Final_Instrument_URL || '') : (o.Instrument_URL || ''),
                hasFinals ? (e.Final_Enactment_Date || '') : (o.Enactment_Date || ''),
                hasFinals ? (e.Final_Date_of_Entry_in_Force || '') : (o.Date_of_Entry_in_Force || o['Date of Entry in Force'] || ''),
                hasFinals ? (e.Final_Repeal_Year || '') : (o.Repeal_Year || ''),
                hasFinals ? (e.Final_Current_Status || '') : (o.Current_Status || o['Current Status'] || ''),
                hasFinals ? (e.Final_Public || '') : (o.Public || ''),
                hasFinals ? (e.Final_Flag || '') : (o.Flag || ''),
            ]);
        }

        // ── EVIDENCE SHEET — exact column order via aoa_to_sheet ──
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

        const evidenceAoa = [EVIDENCE_HEADERS];
        for (const row of rows) {
            const e = row.evidence_json || {};
            const o = row.output_json || {};
            const input = row.input_data || {};
            evidenceAoa.push([
                e.Row_Index || row.row_index,
                e.Economy || o.Economy || input.Economy || '',
                e.Economy_Code || o.Economy_Code || '',
                e.Legal_basis_verbatim || input.Legal_basis || input['Legal basis'] || '',
                e.Query_1 || '',
                e.Query_2 || '',
                e.Query_3 || '',
                e.URLs_Considered || '',
                e.Selected_Source_URLs || '',
                e.Source_Tier || e.Tier || '',
                e.Public_Access || '',
                e.Raw_Official_Title_As_Source || '',
                e.Normalized_Title_Used || '',
                e.Language_Justification || '',
                e.Instrument_URL_Support || '',
                e.Enactment_Support || '',
                e.EntryIntoForce_Support || '',
                e.Status_Support || '',
                e.Missing_Conflict_Reason || e['Missing/Conflict_Reason'] || '',
                e.Normalization_Notes || '',
                e.Final_Language_Doc ?? o.Language_Doc ?? '',
                e.Final_Instrument_Full_Name_Original_Language ?? o.Instrument_Full_Name_Original_Language ?? '',
                e.Final_Instrument_Published_Name ?? o.Instrument_Published_Name ?? '',
                e.Final_Instrument_URL ?? o.Instrument_URL ?? '',
                e.Final_Enactment_Date ?? o.Enactment_Date ?? '',
                e.Final_Date_of_Entry_in_Force ?? (o.Date_of_Entry_in_Force || o['Date of Entry in Force'] || ''),
                e.Final_Repeal_Year ?? o.Repeal_Year ?? '',
                e.Final_Current_Status ?? (o.Current_Status || o['Current Status'] || ''),
                e.Final_Public ?? o.Public ?? '',
                e.Final_Flag ?? o.Flag ?? '',
            ]);
        }

        const wb = XLSX.utils.book_new();
        const wsOutput = XLSX.utils.aoa_to_sheet(outputAoa);
        XLSX.utils.book_append_sheet(wb, wsOutput, 'Output');
        const wsEvidence = XLSX.utils.aoa_to_sheet(evidenceAoa);
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