import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

// Fix 9: Full spec text derived from the authoritative specification document.
// This replaces the rough approximation with the complete, structured spec content.
const DEFAULT_SPEC = `# Legal Metadata Extractor — Authoritative Specification

## 1. Purpose
This specification defines the rules for extracting legal instrument metadata from structured input data using AI-assisted web search and analysis. It is the single source of truth for how the extraction engine processes each row.

## 2. Input File Requirements
File 1 (Excel) must contain the following columns in any order:
- Owner
- Economy
- Legal basis
- Question
- Topic

Each row represents one legal instrument to be identified and characterized.

## 3. Output File Requirements
File 2 (Excel) with two sheets:

### Sheet 1: Output
Columns (exact order):
1. Owner — copied from input
2. Economy — copied from input
3. Economy_Code — looked up from economy_codes mapping (case-insensitive, trimmed). If not found, leave blank and flag.
4. Legal_basis — copied from input
5. Question — copied from input
6. Topic — copied from input
7. Instrument_Title — the official title of the identified legal instrument, normalized (excess whitespace removed, major words capitalized, official abbreviations preserved)
8. Instrument_URL — the source URL where the instrument text or reference was found
9. Instrument_Date — in YYYY-MM-DD format. If only year is known, use YYYY-01-01. If unknown, leave blank and add DATE_UNCERTAIN flag.
10. Instrument_Type — the type/category of the legal instrument (e.g., Act, Regulation, Decree, Order, Directive, Treaty, etc.)
11. Extraction_Status — one of: success | partial | failed
12. Confidence_Score — a decimal between 0.0 and 1.0 (see scoring rules below)
13. Processing_Notes — any relevant notes about the extraction

### Sheet 2: Evidence
Columns (exact order):
1. Row_Index — 1-based row number matching the input
2. Query_1 — [Legal basis] + [Economy] + "official text"
3. Query_2 — [Legal basis] + [Economy] + "legislation database"
4. Query_3 — [Topic] + [Economy] + "legal instrument" + [Question keywords]
5. URLs_Considered — semicolon-separated list of all URLs reviewed
6. Selected_Source_URLs — semicolon-separated list of URLs from which data was extracted
7. Tier — the tier of the primary source (1–4)
8. Raw_Evidence — the raw text extracted or referenced from the source
9. Extraction_Logic — a brief explanation of the reasoning used to select and extract the instrument
10. Flags — comma-separated list of applicable flags

## 4. Processing Rules

### 4.1 Economy Code Mapping
- Use the economy_codes table for mapping economy names to codes
- Matching is case-insensitive and whitespace-trimmed
- If no match is found, leave Economy_Code blank and add flag NO_ECONOMY_CODE

### 4.2 Query Generation
For each row, generate exactly 3 search queries:
- Query_1: [Legal basis] [Economy] official text
- Query_2: [Legal basis] [Economy] legislation database
- Query_3: [Topic] [Economy] legal instrument [Question keywords]

### 4.3 Source Tier System
Sources are ranked by reliability:
- Tier 1: Official government sources (.gov, .govt, official gazette domains)
- Tier 2: International organization sources (WTO, UN, OECD, World Bank, etc.)
- Tier 3: Academic and legal databases (LexisNexis, Westlaw, HeinOnline, university repositories)
- Tier 4: Other reputable sources (legal news, professional associations, NGOs)

Always prefer higher-tier sources. Record the tier of the primary source used.

### 4.4 Instrument Title Normalization
- Remove excess whitespace (multiple spaces, leading/trailing)
- Capitalize the first letter of each major word (articles, prepositions under 4 letters remain lowercase unless at the start)
- Keep official abbreviations intact (e.g., EU, WTO, GATT)
- Preserve the original language title if the instrument is not in English

### 4.5 Date Format
- Standard output: YYYY-MM-DD
- If only year is known: YYYY-01-01
- If only year and month: YYYY-MM-01
- If date is completely unknown: leave blank and add flag DATE_UNCERTAIN

### 4.6 Multilingual Requirements
- Always generate search queries in English
- Additionally generate queries in the local official language(s) of the economy when applicable
- If the primary source is in a non-English language, add flag TRANSLATION_NEEDED

### 4.7 Confidence Scoring
- 1.0: Perfect match — instrument found on a Tier 1 source with full metadata (title, date, URL, type all verified)
- 0.8–0.9: Strong match — from Tier 1–2 sources with most metadata confirmed
- 0.6–0.7: Partial match — from Tier 2–3 sources, or some metadata fields uncertain
- 0.4–0.5: Weak match — from Tier 3–4 sources, or significant uncertainty in identification
- 0.1–0.3: Low confidence — best guess based on limited evidence, manual review strongly recommended
- 0.0: No match found — extraction failed entirely

### 4.8 Flags Reference
- NO_ECONOMY_CODE: Economy code mapping not found in the lookup table
- DATE_UNCERTAIN: Date could not be fully determined from available sources
- MULTIPLE_INSTRUMENTS: Multiple potentially relevant instruments were found; the most likely one was selected
- TRANSLATION_NEEDED: Primary source is in a non-English language
- MANUAL_REVIEW: Result requires human verification due to ambiguity or low confidence
- LOW_CONFIDENCE: Confidence score is below 0.4
- PARSE_ERROR: LLM response could not be parsed (system flag)
- AMENDED: The instrument has been amended; the most recent version was used
- SUPERSEDED: The instrument may have been superseded by a newer one

## 5. Error Handling
- If the LLM cannot identify any instrument: set Extraction_Status=failed, Confidence_Score=0.0, add MANUAL_REVIEW flag
- If multiple instruments are found: select the most relevant one, set flag MULTIPLE_INSTRUMENTS, note alternatives in Processing_Notes
- If a source URL is no longer accessible: note in Processing_Notes, try alternative sources`;

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
                    const newSpec = await base44.entities.Spec.create({
                        is_active: true,
                        current_text: DEFAULT_SPEC,
                        title: 'Default Legal Metadata Extractor Spec',
                        updated_by_email: user.email
                    });
                    
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
                
                let specs = await base44.entities.Spec.filter({ is_active: true });
                let spec;
                
                if (specs.length === 0) {
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
                
                const versions = await base44.entities.SpecVersion.filter({ spec_id: spec.id });
                const maxVersion = versions.reduce((max, v) => Math.max(max, v.version_number || 0), 0);
                
                const newVersion = await base44.entities.SpecVersion.create({
                    spec_id: spec.id,
                    version_number: maxVersion + 1,
                    spec_text: spec_text,
                    change_note: change_note || 'Updated specification',
                    created_by_email: user.email
                });
                
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
                
                const versions = await base44.entities.SpecVersion.filter({ spec_id: spec.id });
                const maxVersion = versions.reduce((max, v) => Math.max(max, v.version_number || 0), 0);
                
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

            // Fix 9: Action to restore from uploaded docx
            case 'restoreFromFile': {
                const { file_url } = params;
                if (!file_url) {
                    return Response.json({ error: 'file_url is required' }, { status: 400 });
                }

                // Use the built-in extraction to get text from the uploaded file
                const extractResult = await base44.integrations.Core.ExtractDataFromUploadedFile({
                    file_url,
                    json_schema: {
                        type: 'object',
                        properties: {
                            full_text: { type: 'string', description: 'The complete text content of the document, preserving structure and formatting as markdown' }
                        }
                    }
                });

                if (extractResult.status !== 'success' || !extractResult.output?.full_text) {
                    return Response.json({ error: 'Failed to extract text from file' }, { status: 400 });
                }

                const specText = extractResult.output.full_text;

                const specs = await base44.entities.Spec.filter({ is_active: true });
                let spec;

                if (specs.length === 0) {
                    spec = await base44.entities.Spec.create({
                        is_active: true,
                        current_text: specText,
                        title: 'Spec from uploaded document',
                        updated_by_email: user.email
                    });
                } else {
                    spec = specs[0];
                    await base44.entities.Spec.update(spec.id, {
                        current_text: specText,
                        updated_by_email: user.email
                    });
                }

                const versions = await base44.entities.SpecVersion.filter({ spec_id: spec.id });
                const maxVersion = versions.reduce((max, v) => Math.max(max, v.version_number || 0), 0);

                await base44.entities.SpecVersion.create({
                    spec_id: spec.id,
                    version_number: maxVersion + 1,
                    spec_text: specText,
                    change_note: 'Restored from uploaded document',
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