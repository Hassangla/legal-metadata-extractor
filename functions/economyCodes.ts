import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

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

            case 'importFromCsv': {
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required' }, { status: 403 });
                }

                const { file_url } = params;
                if (!file_url) {
                    return Response.json({ error: 'file_url is required' }, { status: 400 });
                }

                // Fetch CSV file
                const response = await fetch(file_url);
                if (!response.ok) {
                    return Response.json({ error: 'Failed to fetch CSV file' }, { status: 500 });
                }

                const csvText = await response.text();
                const lines = csvText.split(/\r?\n/).filter(l => l.trim());

                if (lines.length < 2) {
                    return Response.json({ error: 'CSV file is empty or has no data rows' }, { status: 400 });
                }

                // Parse header
                const headerLine = lines[0];
                const headers = headerLine.split(',').map(h => h.trim().toLowerCase().replace(/^["']|["']$/g, ''));

                const economyIdx = headers.findIndex(h => h === 'economy');
                const codeIdx = headers.findIndex(h => h === 'economy_code' || h === 'code' || h === 'economy code');

                if (economyIdx === -1 || codeIdx === -1) {
                    return Response.json({ 
                        error: `CSV must have 'economy' and 'economy_code' columns. Found: ${headers.join(', ')}` 
                    }, { status: 400 });
                }

                // Parse rows
                const parsedData = [];
                for (let i = 1; i < lines.length; i++) {
                    const line = lines[i].trim();
                    if (!line) continue;

                    // Simple CSV split (handles quoted values)
                    const parts = [];
                    let current = '';
                    let inQuotes = false;
                    for (let c = 0; c < line.length; c++) {
                        const ch = line[c];
                        if (ch === '"' || ch === "'") {
                            inQuotes = !inQuotes;
                        } else if (ch === ',' && !inQuotes) {
                            parts.push(current.trim());
                            current = '';
                        } else {
                            current += ch;
                        }
                    }
                    parts.push(current.trim());

                    const economy = parts[economyIdx]?.replace(/^["']|["']$/g, '').trim();
                    const code = parts[codeIdx]?.replace(/^["']|["']$/g, '').trim();

                    if (economy && code) {
                        parsedData.push({ economy, economy_code: code });
                    }
                }

                if (parsedData.length === 0) {
                    return Response.json({ error: 'No valid rows found in CSV' }, { status: 400 });
                }

                // Clear existing
                const existing = await base44.entities.EconomyCode.list();
                for (const code of existing) {
                    await base44.entities.EconomyCode.delete(code.id);
                }

                // Import
                let imported = 0;
                for (const item of parsedData) {
                    await base44.entities.EconomyCode.create({
                        economy: item.economy.toLowerCase().trim(),
                        economy_code: item.economy_code.trim()
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