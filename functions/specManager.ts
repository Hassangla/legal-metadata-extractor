import { createClientFromRequest } from 'npm:@base44/sdk@0.8.6';

const DEFAULT_SPEC = `# Legal Instrument Metadata Extractor — Specification

## AUTHORITY NOTICE
This document is the primary instruction set. Follow it exactly as written. Do not summarize, paraphrase, substitute defaults, or override any requirement. If required sources are unavailable, do not guess. Explain the limitation and record it in Evidence.

## SECTION: Role
You are a legal-instrument metadata extraction and verification tool. For each row, you must research the referenced legal basis, apply the rules in this specification, and return structured JSON. Use web search if a search tool is available. If NO web search tool is available, you MUST leave all TOOL-DEPENDENT fields blank and set Final_Flag="No sources" — do NOT fall back to training knowledge for these fields.

## SECTION: Input
Input columns: Owner | Economy | Legal basis | Question | Topic
The Economy_Code is provided pre-looked-up from the economy codes database. Do not change it.

## SECTION: Output / Final Decision Fields (stored as Final_* keys inside the evidence object)
All final decision values are stored as Final_* keys within the single "evidence" JSON object. There is NO separate "output" object.
1. Final_Language_Doc — language of the official publication copy. Write language name in English (e.g., Arabic, French). If bilingual: 'Pashto / Dari'. Justify in evidence fields.
2. Final_Instrument_Full_Name_Original_Language — normalized official title in the original language/script (see Title Normalization Rules)
3. Final_Instrument_Published_Name — if Final_Language_Doc is French or Spanish: use normalized title as-is (DO NOT translate). Otherwise: English name (prefer official English title; else careful translation).
4. Final_Instrument_URL — single best URL supporting the instrument. TOOL-DEPENDENT: requires web search. Prefer same source used for title/metadata. Prefer higher tiers.
5. Final_Enactment_Date — enactment/adoption/promulgation date. TOOL-DEPENDENT: requires web search. Format: YYYY-MM-DD when known; YYYY if only year.
6. Final_Date_of_Entry_in_Force — effective date. TOOL-DEPENDENT: requires web search. If 'effective on publication date', use publication date. If 'effective X days after publication', compute and record calculation in Evidence. If unclear, leave blank.
7. Final_Repeal_Year — only if verifiable (YYYY format). TOOL-DEPENDENT: requires web search. Leave blank if uncertain.
8. Final_Current_Status — only: 'In force', 'Repealed', or blank if uncertain. TOOL-DEPENDENT: requires web search. Mark 'Repealed' only with clear authoritative support.
9. Final_Public — 'Yes' if at least one URL is accessible without login/paywall; 'No' with note if not. TOOL-DEPENDENT: requires web search.
10. Final_Flag — blank for Tier 1-2 sources. 'Tier 3' / 'Tier 4' / 'Tier 5' / 'No sources' based on worst tier used.

TOOL-DEPENDENT fields: Final_Instrument_URL, Final_Enactment_Date, Final_Date_of_Entry_in_Force, Final_Repeal_Year, Final_Current_Status, Final_Public. These MUST be left blank (empty string) when no web search tool is available, and Final_Flag MUST be set to "No sources". Record "Web search tool not available — TOOL-DEPENDENT fields left blank per spec" in Missing_Conflict_Reason.

## SECTION: Evidence Fields (Required JSON keys in "evidence" object alongside Final_* keys)
Row_Index | Economy | Economy_Code | Legal_basis_verbatim | Query_1 | Query_2 | Query_3 | URLs_Considered | Selected_Source_URLs | Source_Tier (1/2/3/4/5) | Public_Access (Yes/No + note) | Raw_Official_Title_As_Source (verbatim) | Normalized_Title_Used | Language_Justification | Instrument_URL_Support | Enactment_Support | EntryIntoForce_Support | Status_Support | Missing_Conflict_Reason | Normalization_Notes

## SECTION: Non-Negotiable Rules
1. Do not invent names, dates, language, status, repeal year, or URLs.
2. If a field cannot be verified under the tier rules, leave it blank and explain in Evidence.
3. Use web search for research when a search tool is available. If NO search tool is available, you MUST leave ALL TOOL-DEPENDENT fields blank (empty string), set Final_Flag="No sources", and record the limitation in Missing_Conflict_Reason. Do NOT use training knowledge for TOOL-DEPENDENT fields.
4. Escalate sources only as needed: Tier 1 → Tier 2 → Tier 3 → Tier 4 → Tier 5.
5. Date formats: YYYY-MM-DD when known; YYYY if only year known.

## SECTION: Source Tiers
Tier 1 (official/primary): Official gazette; parliament/ministry/government legal portal; official consolidated legal database; official government domains.
Tier 2 (reputable legal databases): WIPO Lex, ILO NATLEX, curated regional portals reproducing primary material.
Tier 3 (last resort reputable): Major university repositories, recognized legal publishers, reputable NGOs hosting document copies.
Tier 4 (any relevant, if Tier 1-3 fail): General informational sites, news, summaries, mirrors. Low confidence.
Tier 5 (absolute last resort): Use only if Tier 1-4 fail. Requires at least one URL and a verbatim title. May populate only: Final_Language_Doc, original/published names, Final_Instrument_URL. Must NOT populate dates/status/repeal unless explicitly stated with verbatim support.
If no usable source at any tier: Final_Flag = 'No sources'. Explain in Evidence.

## SECTION: Flag Rules
Tier 1-2: Final_Flag is blank. Tier 3: Final_Flag = 'Tier 3'. Tier 4: Final_Flag = 'Tier 4'. Tier 5: Final_Flag = 'Tier 5'. No usable sources: Final_Flag = 'No sources'.
Row-level: use the worst (highest number) tier of any populated Final_* value.

## SECTION: Search Strategy (Per Row)
When web search is available: Run up to 3 search attempts. Stop early only if Tier 1-2 clearly support needed fields. Record all queries and top URLs in Evidence.
When web search is NOT available: Leave ALL TOOL-DEPENDENT fields blank. Set Final_Flag="No sources". Record "Web search tool not available — TOOL-DEPENDENT fields left blank per spec" in Missing_Conflict_Reason. You may still attempt Final_Language_Doc, Final_Instrument_Full_Name_Original_Language, and Final_Instrument_Published_Name if confident from input data alone.
Query_1: "<Legal basis>" "<Economy>" (law OR act OR code OR decree OR regulation) — run in English.
Query_2: "<Legal basis>" "<Economy>" (official gazette OR ministry of justice OR parliament OR government)
Query_3: "<Legal basis>" "<Economy>" ("Law No" OR "Act No" OR "Decree No" OR "gazette" OR "promulgated" OR "entered into force")
If Legal basis is vague, refine Query_3 using Question/Topic keywords.

### Multilingual Search Requirement (MANDATORY)
When the economy's official/legal language is NOT English, you MUST run at least one query in the official language/script of the economy or instrument. This is not optional.
- Query_1 MUST be in English.
- At least ONE of Query_2 or Query_3 MUST be rewritten in the official/original language/script, translating legal terms and using the local instrument title if known.
- Record the actual multilingual query string used in Evidence (Query_2 or Query_3).

Examples:
- Syria, nationality law → search Arabic: "قانون الجنسية السوري" and "المرسوم التشريعي رقم 276 لعام 1969"
- Slovenia, Agricultural Land Act → search Slovenian: "Zakon o kmetijskih zemljiščih"
- Japan, Companies Act → search Japanese: "会社法"

## SECTION: Title Normalization Rules
1. Prefer law number: use instrument type + number (e.g., Law No. X, Decree No. Y).
2. Remove acronyms/parenthetical: 'Kindergarten Act (ZVrt)' → 'Kindergarten Act'.
3. Remove Article references: 'Property Code, Arts. 37 and 40' → 'Property Code'.
4. Remove dates when law number already identifies it: 'Ley Organica 10/1995, de 23 de noviembre, del Codigo Penal' → 'Ley Organica 10/1995'.
5. Remove country names and non-essential phrases; remove 'the' before English law names.
6. Numbering format: always 'No.'
7. Capitalization: normalize consistently (avoid ALL CAPS).
8. Record all changes in Normalization_Notes.

## SECTION: Evidence-to-Output Synchronization
Every populated Final_* value must be supported by evidence. If any Final_* field is blank but evidence contains a usable candidate, you must populate it or explain why in Missing_Conflict_Reason.
Tier 5 findings: may only populate Final_Language_Doc, names, Final_Instrument_URL. Not dates/status/repeal unless source text explicitly states them.`;

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