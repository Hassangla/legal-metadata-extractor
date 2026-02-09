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
        } catch (e) {
            return Response.json({ error: 'Invalid request body — expected JSON with file_url' }, { status: 400 });
        }

        const { file_url } = body;
        
        if (!file_url) {
            return Response.json({ error: 'file_url is required but was empty or missing' }, { status: 400 });
        }
        
        let response;
        try {
            response = await fetch(file_url);
        } catch (fetchError) {
            return Response.json({ 
                error: `Could not download the uploaded file. Network error: ${fetchError.message}` 
            }, { status: 500 });
        }

        if (!response.ok) {
            return Response.json({ 
                error: `Could not download the uploaded file. Server returned HTTP ${response.status}. The upload URL may have expired — please try uploading again.` 
            }, { status: 500 });
        }
        
        let arrayBuffer;
        try {
            arrayBuffer = await response.arrayBuffer();
        } catch (e) {
            return Response.json({ 
                error: `File downloaded but could not be read into memory: ${e.message}` 
            }, { status: 500 });
        }

        if (arrayBuffer.byteLength === 0) {
            return Response.json({ error: 'The uploaded file is empty (0 bytes).' }, { status: 400 });
        }
        
        let workbook;
        try {
            workbook = XLSX.read(new Uint8Array(arrayBuffer), { type: 'array' });
        } catch (parseError) {
            return Response.json({ 
                error: `Could not parse Excel file. Make sure it is a valid .xlsx or .xls file. Detail: ${parseError.message}` 
            }, { status: 400 });
        }
        
        if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
            return Response.json({ error: 'Excel file has no sheets.' }, { status: 400 });
        }

        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        
        if (!worksheet) {
            return Response.json({ error: `Sheet "${sheetName}" is empty or could not be read.` }, { status: 400 });
        }

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
                error: 'The first sheet has no data rows. Make sure row 1 has column headers and row 2+ has data.' 
            }, { status: 400 });
        }
        
        const headers = Object.keys(data[0]);
        
        const COLUMN_ALIASES = {
            'Owner':       ['owner'],
            'Economy':     ['economy'],
            'Legal basis': ['legal basis', 'legal_basis', 'legalbasis', 'legal basis '],
            'Question':    ['question'],
            'Topic':       ['topic'],
        };

        function findColumn(headers, aliases) {
            for (const h of headers) {
                const normalized = h.toLowerCase().trim().replace(/_/g, ' ');
                if (aliases.some(a => a === normalized)) return h;
            }
            return null;
        }

        const columnMap = {};
        const missingColumns = [];
        for (const [standard, aliases] of Object.entries(COLUMN_ALIASES)) {
            const found = findColumn(headers, aliases);
            if (found) {
                columnMap[standard] = found;
            } else {
                missingColumns.push(standard);
            }
        }
        
        if (missingColumns.length > 0) {
            return Response.json({ 
                error: `Missing required columns: ${missingColumns.join(', ')}. Found columns: ${headers.join(', ')}`,
                found_columns: headers
            }, { status: 400 });
        }
        
        const normalizedData = data.map(row => {
            return {
                Owner:       String(row[columnMap['Owner']] || '').trim(),
                Economy:     String(row[columnMap['Economy']] || '').trim(),
                Legal_basis: String(row[columnMap['Legal basis']] || '').trim(),
                Question:    String(row[columnMap['Question']] || '').trim(),
                Topic:       String(row[columnMap['Topic']] || '').trim(),
            };
        });

        const filteredData = normalizedData.filter(row => 
            row.Owner || row.Economy || row.Legal_basis || row.Question || row.Topic
        );

        if (filteredData.length === 0) {
            return Response.json({ 
                error: 'All rows are empty. Make sure data starts in row 2.' 
            }, { status: 400 });
        }
        
        return Response.json({ 
            success: true,
            rows: filteredData,
            total_rows: filteredData.length,
            columns: headers
        });
        
    } catch (error) {
        return Response.json({ 
            error: `Unexpected error processing file: ${error.message}` 
        }, { status: 500 });
    }
});