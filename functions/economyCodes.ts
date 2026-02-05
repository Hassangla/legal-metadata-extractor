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
                // Admin only
                if (user.role !== 'admin') {
                    return Response.json({ error: 'Admin access required' }, { status: 403 });
                }
                
                const { data } = params;
                
                if (!data || !Array.isArray(data)) {
                    return Response.json({ error: 'Data array required' }, { status: 400 });
                }
                
                // Clear existing codes
                const existing = await base44.entities.EconomyCode.list();
                for (const code of existing) {
                    await base44.entities.EconomyCode.delete(code.id);
                }
                
                // Import new codes
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