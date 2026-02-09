import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';
import * as XLSX from 'npm:xlsx@0.18.5';

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const { file_url } = await req.json();
        
        if (!file_url) {
            return Response.json({ error: 'File URL required' }, { status: 400 });
        }
        
        // Fetch the file
        const response = await fetch(file_url);
        if (!response.ok) {
            return Response.json({ error: 'Failed to fetch file' }, { status: 500 });
        }
        
        const arrayBuffer = await response.arrayBuffer();
        
        // Parse Excel
        const workbook = XLSX.read(new Uint8Array(arrayBuffer), { type: 'array' });
        
        // Get first sheet
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        
        // Convert to JSON
        const data = XLSX.utils.sheet_to_json(worksheet, { defval: '' });
        
        // Validate required columns
        const requiredColumns = ['Owner', 'Economy', 'Legal basis', 'Question', 'Topic'];
        
        if (data.length === 0) {
            return Response.json({ error: 'File is empty' }, { status: 400 });
        }
        
        const headers = Object.keys(data[0]);
        const missingColumns = requiredColumns.filter(col => 
            !headers.some(h => h.toLowerCase().trim() === col.toLowerCase().trim())
        );
        
        if (missingColumns.length > 0) {
            return Response.json({ 
                error: `Missing required columns: ${missingColumns.join(', ')}`,
                found_columns: headers
            }, { status: 400 });
        }
        
        // Normalize column names
        const normalizedData = data.map(row => {
            const normalized = {};
            for (const [key, value] of Object.entries(row)) {
                // Map to standard column names
                const lowerKey = key.toLowerCase().trim();
                if (lowerKey === 'owner') normalized.Owner = value;
                else if (lowerKey === 'economy') normalized.Economy = value;
                else if (lowerKey === 'legal basis') normalized.Legal_basis = value;
                else if (lowerKey === 'question') normalized.Question = value;
                else if (lowerKey === 'topic') normalized.Topic = value;
                else normalized[key] = value;
            }
            return normalized;
        });
        
        return Response.json({ 
            success: true,
            rows: normalizedData,
            total_rows: normalizedData.length,
            columns: headers
        });
        
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});