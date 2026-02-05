import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

const DEFAULT_SPEC = `# Legal Metadata Extractor Specification

## Input File Requirements
File 1 (Excel) must contain columns: Owner | Economy | Legal basis | Question | Topic

## Output File Requirements
File 2 (Excel) with two sheets:

### Sheet 1: Output
Columns (exact order):
- Owner
- Economy
- Economy_Code
- Legal_basis
- Question
- Topic
- Instrument_Title
- Instrument_URL
- Instrument_Date
- Instrument_Type
- Extraction_Status
- Confidence_Score
- Processing_Notes

### Sheet 2: Evidence
Columns (exact order):
- Row_Index
- Query_1
- Query_2
- Query_3
- URLs_Considered
- Selected_Source_URLs
- Tier
- Raw_Evidence
- Extraction_Logic
- Flags

## Processing Rules

### Economy Code Mapping
- Use economy_codes.csv for mapping
- Match is case-insensitive and trimmed
- If no match found, leave Economy_Code blank and add flag

### Query Generation
- Generate 3 search queries per row
- Query_1: [Legal basis] + [Economy] + "official text"
- Query_2: [Legal basis] + [Economy] + "legislation database"
- Query_3: [Topic] + [Economy] + "legal instrument" + [Question keywords]

### Tier System
- Tier 1: Official government sources (.gov domains)
- Tier 2: International organization sources (WTO, UN, etc.)
- Tier 3: Academic and legal databases
- Tier 4: Other reputable sources

### Instrument Title Normalization
- Remove excess whitespace
- Capitalize first letter of each major word
- Keep official abbreviations intact

### Date Format
- Output format: YYYY-MM-DD
- If only year known: YYYY-01-01
- If date unknown: leave blank, add flag

### Multilingual Requirements
- Generate queries in English
- Also generate queries in local official language(s) when applicable

### Confidence Scoring
- 1.0: Perfect match from Tier 1 source
- 0.8-0.9: Good match from Tier 1-2 sources
- 0.6-0.7: Partial match or Tier 3 source
- 0.4-0.5: Weak match or Tier 4 source
- Below 0.4: Low confidence, manual review needed

### Flags
- NO_ECONOMY_CODE: Economy code mapping not found
- DATE_UNCERTAIN: Date could not be fully determined
- MULTIPLE_INSTRUMENTS: Multiple relevant instruments found
- TRANSLATION_NEEDED: Source in non-English language
- MANUAL_REVIEW: Requires human verification
- LOW_CONFIDENCE: Confidence score below threshold`;

Deno.serve(async (req) => {
    try {
        const base44 = createClientFromRequest(req);
        const user = await base44.auth.me();
        
        if (!user) {
            return Response.json({ error: 'Unauthorized' }, { status: 401 });
        }

        const { action, ...params } = await req.json();

        switch (action) {
            case 'getActive': {
                const specs = await base44.entities.Spec.filter({ is_active: true });
                
                if (specs.length === 0) {
                    // Create default spec
                    const newSpec = await base44.entities.Spec.create({
                        is_active: true,
                        current_text: DEFAULT_SPEC,
                        title: 'Default Legal Metadata Extractor Spec',
                        updated_by_email: user.email
                    });
                    
                    // Create initial version
                    await base44.entities.SpecVersion.create({
                        spec_id: newSpec.id,
                        version_number: 1,
                        spec_text: DEFAULT_SPEC,
                        change_note: 'Initial default specification',
                        created_by_email: user.email
                    });
                    
                    return Response.json({ spec: newSpec });
                }
                
                return Response.json({ spec: specs[0] });
            }
            
            case 'save': {
                const { spec_text, change_note } = params;
                
                if (!spec_text || spec_text.trim().length === 0) {
                    return Response.json({ error: 'Spec text cannot be empty' }, { status: 400 });
                }
                
                // Get active spec
                let specs = await base44.entities.Spec.filter({ is_active: true });
                let spec;
                
                if (specs.length === 0) {
                    // Create new spec
                    spec = await base44.entities.Spec.create({
                        is_active: true,
                        current_text: spec_text,
                        title: 'Legal Metadata Extractor Spec',
                        updated_by_email: user.email
                    });
                } else {
                    spec = specs[0];
                    await base44.entities.Spec.update(spec.id, {
                        current_text: spec_text,
                        updated_by_email: user.email
                    });
                }
                
                // Get latest version number
                const versions = await base44.entities.SpecVersion.filter({ spec_id: spec.id });
                const maxVersion = versions.reduce((max, v) => Math.max(max, v.version_number || 0), 0);
                
                // Create new version
                const newVersion = await base44.entities.SpecVersion.create({
                    spec_id: spec.id,
                    version_number: maxVersion + 1,
                    spec_text: spec_text,
                    change_note: change_note || 'Updated specification',
                    created_by_email: user.email
                });
                
                // Get updated spec
                const updatedSpecs = await base44.entities.Spec.filter({ id: spec.id });
                
                return Response.json({ 
                    spec: updatedSpecs[0], 
                    version: newVersion 
                });
            }
            
            case 'getVersions': {
                const specs = await base44.entities.Spec.filter({ is_active: true });
                if (specs.length === 0) {
                    return Response.json({ versions: [] });
                }
                
                const versions = await base44.entities.SpecVersion.filter({ spec_id: specs[0].id });
                versions.sort((a, b) => (b.version_number || 0) - (a.version_number || 0));
                
                return Response.json({ versions });
            }
            
            case 'getVersion': {
                const { version_id } = params;
                const versions = await base44.entities.SpecVersion.filter({ id: version_id });
                
                if (versions.length === 0) {
                    return Response.json({ error: 'Version not found' }, { status: 404 });
                }
                
                return Response.json({ version: versions[0] });
            }
            
            case 'restoreDefault': {
                const specs = await base44.entities.Spec.filter({ is_active: true });
                let spec;
                
                if (specs.length === 0) {
                    spec = await base44.entities.Spec.create({
                        is_active: true,
                        current_text: DEFAULT_SPEC,
                        title: 'Default Legal Metadata Extractor Spec',
                        updated_by_email: user.email
                    });
                } else {
                    spec = specs[0];
                    await base44.entities.Spec.update(spec.id, {
                        current_text: DEFAULT_SPEC,
                        updated_by_email: user.email
                    });
                }
                
                // Get latest version number
                const versions = await base44.entities.SpecVersion.filter({ spec_id: spec.id });
                const maxVersion = versions.reduce((max, v) => Math.max(max, v.version_number || 0), 0);
                
                // Create restore version
                await base44.entities.SpecVersion.create({
                    spec_id: spec.id,
                    version_number: maxVersion + 1,
                    spec_text: DEFAULT_SPEC,
                    change_note: 'Restored to default specification',
                    created_by_email: user.email
                });
                
                const updatedSpecs = await base44.entities.Spec.filter({ id: spec.id });
                
                return Response.json({ spec: updatedSpecs[0] });
            }
            
            case 'getLatestVersionId': {
                const specs = await base44.entities.Spec.filter({ is_active: true });
                if (specs.length === 0) {
                    return Response.json({ error: 'No active spec found' }, { status: 404 });
                }
                
                const versions = await base44.entities.SpecVersion.filter({ spec_id: specs[0].id });
                if (versions.length === 0) {
                    return Response.json({ error: 'No spec versions found' }, { status: 404 });
                }
                
                versions.sort((a, b) => (b.version_number || 0) - (a.version_number || 0));
                
                return Response.json({ 
                    version_id: versions[0].id,
                    version_number: versions[0].version_number
                });
            }
            
            default:
                return Response.json({ error: 'Unknown action' }, { status: 400 });
        }
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
});