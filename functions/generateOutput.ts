import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';
import * as XLSX from 'npm:xlsx@0.18.5';

// Chunked base64 conversion to avoid stack overflow on large files
function arrayBufferToBase64(buffer) {
    const bytes = new Uint8Array(buffer);
    const CHUNK_SIZE = 32768;
    let binary = '';
    for (let i = 0; i < bytes.length; i += CHUNK_SIZE) {
        const chunk = bytes.subarray(i, Math.min(i + CHUNK_SIZE, bytes.length));
        binary += String.fromCharCode.apply(null, chunk);
    }
    return btoa(binary);
}

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const { job_id } = await req.json();

        // FIX 10: null guard
        if (!job_id) {
            return Response.json({ error: 'job_id is required' }, { status: 400 });
        }
        
        const jobs = await base44.entities.Job.filter({ id: job_id });
        if (jobs.length === 0) {
            return Response.json({ error: 'Job not found' }, { status: 404 });
        }
        
        const job = jobs[0];
        
        const rows = await base44.entities.JobRow.filter({ job_id });
        rows.sort((a, b) => a.row_index - b.row_index);
        
        const outputData = rows.map(row => {
            const output = row.output_json || {};
            return {
                'Owner': output.Owner || row.input_data?.Owner || '',
                'Economy': output.Economy || row.input_data?.Economy || '',
                'Economy_Code': output.Economy_Code || '',
                'Legal_basis': output.Legal_basis || row.input_data?.Legal_basis || row.input_data?.['Legal basis'] || '',
                'Question': output.Question || row.input_data?.Question || '',
                'Topic': output.Topic || row.input_data?.Topic || '',
                'Instrument_Title': output.Instrument_Title || '',
                'Instrument_URL': output.Instrument_URL || '',
                'Instrument_Date': output.Instrument_Date || '',
                'Instrument_Type': output.Instrument_Type || '',
                'Extraction_Status': output.Extraction_Status || (row.status === 'error' ? 'error' : ''),
                'Confidence_Score': output.Confidence_Score || '',
                'Processing_Notes': output.Processing_Notes || row.error_message || ''
            };
        });
        
        const evidenceData = rows.map(row => {
            const evidence = row.evidence_json || {};
            return {
                'Row_Index': evidence.Row_Index || row.row_index,
                'Query_1': evidence.Query_1 || '',
                'Query_2': evidence.Query_2 || '',
                'Query_3': evidence.Query_3 || '',
                'URLs_Considered': evidence.URLs_Considered || '',
                'Selected_Source_URLs': evidence.Selected_Source_URLs || '',
                'Tier': evidence.Tier || '',
                'Raw_Evidence': evidence.Raw_Evidence || '',
                'Extraction_Logic': evidence.Extraction_Logic || '',
                'Flags': evidence.Flags || ''
            };
        });
        
        const wb = XLSX.utils.book_new();
        
        const wsOutput = XLSX.utils.json_to_sheet(outputData);
        XLSX.utils.book_append_sheet(wb, wsOutput, 'Output');
        
        const wsEvidence = XLSX.utils.json_to_sheet(evidenceData);
        XLSX.utils.book_append_sheet(wb, wsEvidence, 'Evidence');
        
        const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
        
        // FIX 10: safe chunked base64 conversion
        const base64Data = arrayBufferToBase64(buffer);
        
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
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