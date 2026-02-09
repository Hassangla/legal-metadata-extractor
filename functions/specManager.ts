import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

const DEFAULT_SPEC = `# Legal Metadata Extractor — Full Specification

## 1. Overview

This tool extracts structured metadata from legal instruments referenced in an input Excel file. It uses an LLM with web search capabilities to locate authoritative sources, extract key attributes, and produce a standardised output spreadsheet with full evidence traceability.

## 2. Input File Requirements

File 1 (Excel .xlsx) must contain the following columns in the first sheet:

| Column | Description |
|---|---|
| Owner | Entity or person responsible |
| Economy | Country / territory name |
| Legal basis | Title or identifier of the legal instrument |
| Question | The compliance question this row relates to |
| Topic | Thematic area (e.g. trade, customs) |

Additional columns are ignored but preserved in evidence.

## 3. Output File Requirements

File 2 (Excel .xlsx) with two sheets:

### Sheet 1: Output
Columns (exact order):
- Owner — copied from input
- Economy — copied from input
- Economy_Code — looked up from economy_codes mapping (post-processed, not model-generated)
- Legal_basis — copied from input
- Question — copied from input
- Topic — copied from input
- Instrument_Title — normalised official title of the instrument
- Instrument_URL — best authoritative URL found
- Instrument_Date — in YYYY-MM-DD format
- Instrument_Type — e.g. Act, Regulation, Decree, Order, Treaty, etc.
- Extraction_Status — one of: success, partial, failed
- Confidence_Score — decimal 0.0–1.0
- Processing_Notes — free-text notes on extraction outcome

### Sheet 2: Evidence
Columns (exact order):
- Row_Index — 1-based row number matching the input
- Query_1 — [Legal basis] + [Economy] + "official text"
- Query_2 — [Legal basis] + [Economy] + "legislation database"
- Query_3 — [Topic] + [Economy] + "legal instrument" + [Question keywords]
- URLs_Considered — semicolon-separated list of URLs reviewed
- Selected_Source_URLs — the URL(s) ultimately used
- Tier — source tier (1–4)
- Raw_Evidence — relevant raw text extracted from source
- Extraction_Logic — reasoning chain for choosing the instrument
- Flags — semicolon-separated flags (see §7)

## 4. Processing Rules

### 4.1 Economy Code Mapping
- Use the imported economy_codes table for mapping.
- Match is case-insensitive and whitespace-trimmed.
- If no match is found, leave Economy_Code blank and add flag NO_ECONOMY_CODE.
- **Important**: The model must NOT guess or hallucinate economy codes. Post-processing injects codes.

### 4.2 Query Generation
For each input row, generate three search queries:
1. Query_1: [Legal basis] [Economy] official text
2. Query_2: [Legal basis] [Economy] legislation database
3. Query_3: [Topic] [Economy] legal instrument [Question keywords]

### 4.3 Source Tier System
- Tier 1: Official government sources (.gov, .gov.xx domains)
- Tier 2: International organisation sources (WTO, UNCTAD, UN, WIPO, etc.)
- Tier 3: Academic and legal databases (e.g. FAOLEX, ILO NATLEX)
- Tier 4: Other reputable sources (law firm publications, news)

Always prefer the highest (lowest-numbered) tier available.

### 4.4 Instrument Title Normalisation
- Remove excess whitespace.
- Capitalise the first letter of each major word.
- Preserve official abbreviations (e.g. "GATT", "SPS").
- If the source title is in a non-English language, also provide an English translation in parentheses.

### 4.5 Date Format
- Output format: YYYY-MM-DD
- If only year known: YYYY-01-01
- If date unknown: leave blank and add flag DATE_UNCERTAIN

### 4.6 Multilingual Considerations
- Generate queries primarily in English.
- When the economy's official language is not English, also consider queries in the local language.
- Flag TRANSLATION_NEEDED if the primary source is non-English.

## 5. Confidence Scoring

| Score Range | Meaning |
|---|---|
| 0.9–1.0 | Perfect match from Tier 1 source, all fields populated |
| 0.7–0.89 | Good match from Tier 1–2 sources, minor fields may be uncertain |
| 0.5–0.69 | Partial match or Tier 3 source, some fields inferred |
| 0.3–0.49 | Weak match, Tier 4 source, multiple fields uncertain |
| < 0.3 | Low confidence, manual review strongly recommended |

## 6. Retry and Error Handling
- Transient API failures (429, 5xx) trigger automatic retries with exponential backoff.
- After exhausting retries, the row is marked Extraction_Status = "failed" with the error in Processing_Notes.
- Parse errors from the LLM result in PARSE_ERROR flag and Extraction_Status = "failed".

## 7. Flags Reference

| Flag | Meaning |
|---|---|
| NO_ECONOMY_CODE | Economy code mapping not found |
| DATE_UNCERTAIN | Date could not be fully determined |
| MULTIPLE_INSTRUMENTS | Multiple relevant instruments found |
| TRANSLATION_NEEDED | Primary source in non-English language |
| MANUAL_REVIEW | Requires human verification |
| LOW_CONFIDENCE | Confidence score below 0.5 |
| PARSE_ERROR | LLM response could not be parsed as JSON |

## 8. JSON Output Contract

The LLM must return a single JSON object (no markdown, no fences) with exactly two keys:
- "output" — object matching Sheet 1 columns (except Economy_Code, injected post-process)
- "evidence" — object matching Sheet 2 columns`;

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

            case 'restoreFromDocx': {
                const { file_url } = params;
                if (!file_url) {
                    return Response.json({ error: 'file_url is required' }, { status: 400 });
                }

                // Extract text from the uploaded docx using the built-in integration
                const extracted = await base44.integrations.Core.ExtractDataFromUploadedFile({
                    file_url,
                    json_schema: {
                        type: 'object',
                        properties: {
                            spec_text: { type: 'string', description: 'The full plain text content of the specification document' }
                        }
                    }
                });

                if (extracted.status !== 'success' || !extracted.output?.spec_text) {
                    return Response.json({ error: 'Failed to extract text from document' }, { status: 400 });
                }

                const docxText = extracted.output.spec_text;

                let specs = await base44.entities.Spec.filter({ is_active: true });
                let spec;
                if (specs.length === 0) {
                    spec = await base44.entities.Spec.create({
                        is_active: true,
                        current_text: docxText,
                        title: 'Legal Metadata Extractor Spec',
                        updated_by_email: user.email
                    });
                } else {
                    spec = specs[0];
                    await base44.entities.Spec.update(spec.id, {
                        current_text: docxText,
                        updated_by_email: user.email
                    });
                }

                const versions = await base44.entities.SpecVersion.filter({ spec_id: spec.id });
                const maxVersion = versions.reduce((max, v) => Math.max(max, v.version_number || 0), 0);

                await base44.entities.SpecVersion.create({
                    spec_id: spec.id,
                    version_number: maxVersion + 1,
                    spec_text: docxText,
                    change_note: 'Restored from uploaded document',
                    created_by_email: user.email
                });

                const updatedSpecs = await base44.entities.Spec.filter({ id: spec.id });
                return Response.json({ spec: updatedSpecs[0] });
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