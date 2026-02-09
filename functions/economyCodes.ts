import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

// Fix 12: Simple CSV parser — no AI extraction needed
function parseCSV(text) {
    const lines = text.split(/\r?\n/).filter(line => line.trim().length > 0);
    if (lines.length === 0) return [];

    // Detect delimiter (comma or semicolon)
    const firstLine = lines[0];
    const delimiter = firstLine.includes(';') && !firstLine.includes(',') ? ';' : ',';

    const headers = lines[0].split(delimiter).map(h => h.trim().replace(/^["']|["']$/g, '').toLowerCase());

    const economyIdx = headers.findIndex(h => h === 'economy' || h === 'economy_name' || h === 'name');
    const codeIdx = headers.findIndex(h => h === 'economy_code' || h === 'code' || h === 'iso_code');

    if (economyIdx === -1 || codeIdx === -1) {
        return null; // Headers not found
    }

    const rows = [];
    for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(delimiter).map(v => v.trim().replace(/^["']|["']$/g, ''));
        const economy = values[economyIdx];
        const code = values[codeIdx];
        if (economy && code) {
            rows.push({ economy: economy.toLowerCase().trim(), economy_code: code.trim() });
        }
    }
    return rows;
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
            
            case 'import': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required' }, { status: 403 });
                }
                
                const { data } = params;
                
                if (!data || !Array.isArray(data)) {
                    return Response.json({ error: 'Data array required' }, { status: 400 });
                }
                
                const existing = await base44.entities.EconomyCode.list();
                for (const code of existing) {
                    await base44.entities.EconomyCode.delete(code.id);
                }
                
                let imported = 0;
                for (const item of data) {
                    if (item.economy && item.economy_code) {
                        await base44.entities.EconomyCode.create({
                            economy: item.economy.toLowerCase().trim(),
                            economy_code: item.economy_code.trim()
                        });
                        imported++;
                    }
                }
                
                return Response.json({ success: true, imported });
            }

            // Fix 12: Deterministic CSV import — no AI extraction
            case 'importFromCsv': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required' }, { status: 403 });
                }

                const { file_url } = params;
                if (!file_url) {
                    return Response.json({ error: 'file_url is required' }, { status: 400 });
                }

                // Fetch the CSV file
                const fileResponse = await fetch(file_url);
                if (!fileResponse.ok) {
                    return Response.json({ error: 'Failed to fetch file' }, { status: 400 });
                }
                const csvText = await fileResponse.text();

                const rows = parseCSV(csvText);
                if (rows === null) {
                    return Response.json({ 
                        error: 'Could not find economy and economy_code columns in CSV. Expected headers: economy (or name), economy_code (or code).' 
                    }, { status: 400 });
                }

                if (rows.length === 0) {
                    return Response.json({ error: 'No valid data rows found in CSV' }, { status: 400 });
                }

                // Clear existing
                const existing = await base44.entities.EconomyCode.list();
                for (const code of existing) {
                    await base44.entities.EconomyCode.delete(code.id);
                }

                // Insert new
                let imported = 0;
                for (const item of rows) {
                    await base44.entities.EconomyCode.create({
                        economy: item.economy,
                        economy_code: item.economy_code
                    });
                    imported++;
                }

                return Response.json({ success: true, imported });
            }
            
            case 'lookup': {
                const { economy } = params;
                
                if (!economy) {
                    return Response.json({ error: 'Economy name required' }, { status: 400 });
                }
                
                const codes = await base44.entities.EconomyCode.list();
                const match = codes.find(c => 
                    c.economy.toLowerCase().trim() === economy.toLowerCase().trim()
                );
                
                return Response.json({ 
                    found: !!match,
                    economy_code: match?.economy_code || null
                });
            }
            
            default:
                return Response.json({ error: 'Unknown action' }, { status: 400 });
        }
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});