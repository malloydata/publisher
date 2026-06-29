# Rubric: Style & Naming

**Dimension:** `style`
**Rules:** 3

Style findings are almost always `nit` severity, aggregate them in the output rather than listing per-site.

For every rule, the linked instruction-skill section is the canonical source for rationale and examples.

---

## Y-01: Prefer semantic aliases over backticked reserved words

- **Severity:** minor (non-blocking) · **Category:** style · LLM-judgment
- **Detection:** LLM, flag any dimension declared as `` `reserved_word` is `reserved_word` `` (the backticked-passthrough pattern) and suggest a semantic alias
- **Fix:** move the raw column to `internal:` and add a re-aliased dimension with a business name
- **See:** `skill:gotchas-modeling` § Reserved Words, Backtick Them

---

## Y-02: Use business-language naming

- **Severity:** nit (non-blocking) · **Category:** style · LLM-judgment
- **Detection:** LLM judgment informed by, very short names (≤3 chars), hungarian-prefix abbreviations, inconsistent convention within a file. Conventions:
  - **Measures:** suffix with what they measure when helpful (`_count`, `_total`, `_rate`, `_avg`)
  - **Booleans:** prefix with `is_` or `has_`
  - **Timestamps:** suffix with `_at`
  - **Dates (date-only):** suffix with `_date`
  - **IDs:** suffix with `_id`
  - **Avoid:** abbreviations unless they're universal domain terms
- **Fix:** rename to business language; surface in "Suggested Follow-ups" since these are non-blocking
- **Note:** strong personal-preference factor, aggregate into "Suggested Follow-ups" rather than emitting per-site, and only when noise rises above project-wide patterns. Don't drown the review in style nits.
- **See:** `skill:malloy-document` § Writing Doc Strings for Retrieval (naming-as-discoverability is implicit; weak counterpart, naming conventions are a team-style choice)

---

## Y-03: Use one consistent `join_one:` style across the project

- **Severity:** nit (non-blocking) · **Category:** style · LLM-judgment
- **Detection:** flag a `join_one:` only when it deviates from the project's prevailing style (a single `on` form in a file/package that otherwise uses `with`, or vice versa). Skip when both styles are mixed roughly evenly, or when higher-value findings dominate.
- **Fix:** match the prevailing style. Switching to `with` requires the target to declare `primary_key:` (also S-02's recommendation).
- **Never promote above `nit`.** Both forms produce equivalent SQL, see the C-07 entry in `rubric-correctness.md`'s "Rules we dropped" section. The actual silent-correctness hazard (declared PK has duplicates) is `rubric-correctness.md` § C-12, not this rule.
- **See:** `skill:malloy-model` § Join Syntax

---

## Aggregation in the output

Rather than listing every occurrence inline, roll style findings up in "Suggested Follow-ups":

```
## Suggested Follow-ups (non-blocking)

- 34 naming nits aggregated, see Y-series findings in Detailed Findings.
  - Most common: abbreviated measure names (8), missing `is_`/`has_` boolean prefix (6), timestamps without `_at` suffix (5).
- 3 backticked-passthrough dimensions (Y-01), recommend aliasing to business names.
```

This keeps the Top Issues section free of noise while still surfacing the pattern for a cleanup pass.
