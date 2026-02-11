# Project review against target verifier specification

This review compares the current implementation to the provided product specification for the legal instrument verifier.

## Overall

The project is **mostly aligned at the framework level** (input validation, two-sheet export, evidence + final fields, tier concepts, anti-invention posture), but there are several **material compliance gaps** that could produce outputs that do not strictly match the described behavior.

## Strengths / matches

1. Input parser enforces required input columns (`Owner`, `Economy`, `Legal basis`, `Question`, `Topic`).
2. Processing prompt enforces 3-search-query structure and includes multilingual-search instructions.
3. Output is generated with two sheets (`Output`, `Evidence`) and expected output column order.
4. Evidence includes required audit fields and `Final_*` mirror fields.
5. Server-side post-processing enforces key anti-hallucination controls:
   - blanks TOOL-DEPENDENT fields when real web search isn't available,
   - URL provenance checks against tool-returned URLs,
   - no-orphan promotion for some title/language fields.

## Mismatches / risks

### 1) Economy code lookup is not strict CSV mapping (spec mismatch)
- Spec says lookup from `economy_codes.csv` using trim + case-insensitive matching.
- Current implementation uses a database entity (`EconomyCode`) and also applies aliases/fuzzy matching logic, which can map values beyond strict case-insensitive exact matching.
- Impact: Potentially non-deterministic or over-aggressive mappings compared to spec.

### 2) Query_3 refinement rule for vague legal basis is only partially implemented
- Spec says if legal basis is vague, Query_3 must be refined with `Question`/`Topic` keywords.
- Current code only switches to a `Question`/`Topic`-based Query_3 when legal basis is empty (not when it is vague but non-empty).
- Impact: likely under-disambiguation for ambiguous legal basis text.

### 3) Multilingual search is instructed but not hard-validated server-side
- Prompt demands that Query_2 or Query_3 be executed in local language/script and recorded.
- There is no post-validation that query text is actually multilingual (script/language check) or that result URLs came from multilingual attempts.
- Impact: model non-compliance can pass through undetected.

### 4) Tier-5 restriction is incompletely enforced
- Spec says Tier 5 may only populate language/title/published name/URL.
- Server-side Tier-5 enforcement blanks dates/status/repeal, but does **not** blank `Final_Public`.
- Impact: Tier-5 rows may still output `Public`, violating strict allowed-fields rule.

### 5) Flag derivation from worst populated tier is not fully server-enforced
- Spec requires row flag based on the worst tier used for populated values.
- Current server code force-sets for No sources and Tier 5 scenarios, but does not recalculate/verify worst-tier rule comprehensively for Tier 1–4 combinations.
- Impact: possible inconsistency between evidence and final `Flag`.

### 6) Output mirror rule has a fallback path that can bypass Final_*
- Export prefers `Evidence.Final_*` when detected, but falls back to `output_json` if `Final_Flag` is not present.
- Spec says output should be copied from Final_* exactly as a strict consistency mechanism.
- Impact: legacy or malformed rows may drift from evidence-final mirror requirement.

### 7) Public field behavior is not fully deterministic from server checks
- Spec defines `Public=Yes` if at least one supporting URL is accessible without login/paywall, else No + note.
- Server checks URL loadability and may force `No` on failure, but does not always force `Yes` when load check succeeds.
- Impact: `Public` can depend on model output rather than deterministic server-side rule.

## Additional implementation complications

1. Input sheet auto-detection heuristics can pick non-primary sheets if spec-based matching fails; useful in practice but can diverge from strict "single uploaded sheet with exact columns" assumptions.
2. Economy lookup behavior differs between strict lookup endpoint and row processing alias resolution, which may produce hard-to-explain mapping decisions.
3. Strong anti-hallucination logic is present, but many policy constraints still depend on LLM obedience rather than explicit post-validation.

## Suggested remediation priority

1. Enforce strict economy code mapping mode (exact trim + case-insensitive) for row processing.
2. Add explicit "vague legal basis" detection and force Query_3 refinement with Topic/Question.
3. Add server-side multilingual-query compliance checks (with reason logging when absent).
4. In Tier 5 mode, also blank `Final_Public` unless policy is intentionally changed.
5. Recompute `Final_Flag` from evidence tier usage server-side.
6. Remove/fence output fallback so export always mirrors `Final_*`.
7. Deterministically compute `Final_Public` from URL accessibility outcomes.
