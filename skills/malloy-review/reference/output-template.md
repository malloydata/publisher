# Review Results File: Template & Assembly Rules

The output of `/malloy-review` is a single Markdown file, written to `./malloy-review-<YYYYMMDD-HHMMSS>.md` unless `--out` overrides. The file is designed as a **triage document**: the user scans the top, expands only what they want, and can treat the JSON tail as a work-queue.

## Canonical skeleton

```md
# Malloy Model Review: <one-line scope summary>
<branch or path> · <package(s)> · <N files, M lines> · <timestamp>

## Scope
- **Reviewed:** <explicit list of folders/files/globs>
- **Package(s):** <publisher.json packages covered, or "no publisher.json, reviewed folder only">
- **Excluded:** <anything skipped, and why (generated dirs, fixtures, files outside the positional path)>
- **IDE diagnostics:** available for X of Y files (<list of files WITH diagnostics>); Z files had no diagnostics (<list or "most of the scope">, likely unopened in the IDE). Open and re-run for fuller compiler coverage on those files.

## Executive Summary
<3–5 sentences: reconstructed intent of the model/change, what's working, the top 1–3 problems.>

- **Recommendation:** <Approve · Approve-with-comments · Request-changes · Recommend-split>
- **Coverage honesty:** <X files deep-reviewed, Y sampled, Z skimmed, and the rationale for each tier.>
- **Top risks:** <one-line list referencing finding IDs, e.g. "B1 (silent cardinality bug), C2 (governance regression), D-series (docs drift on public measures).">

## Coverage & Risk Map
| File | LOC | Risk | Depth | Notes |
|---|---:|---|---|---|
| models/finance/revenue.malloy | 412 | HIGH | DEEP | New public measures; cardinality change |
| models/shared/_joins.malloy | 180 | HIGH | DEEP | PK hygiene, new `join_one:` |
| models/marketing/attrib.malloy | 2,300 | MED | SAMPLE | Bulk rename; representative sample reviewed |
| tests/fixtures/*.malloy | 600 | LOW | SKIM | Generated; smoke-check only |

Risk formula: `(touches join|PK|access) × LOC × public-surface-membership × (#diagnostics available)`. Depth is the review response, not the risk itself: DEEP = line-by-line, SAMPLE = N% plus hot paths, SKIM = smell-check only.

## Cross-Cutting Themes
<3–5 systemic observations that only a multi-dimension review can surface. Each theme cites finding IDs.>

1. **Join cardinality hygiene regression:** three new `join_one:` declarations lack matching `primary_key:` on the target source (see B1, B2, B3). Systemic fix recommended over per-site patches.
2. **Documentation coverage dropped on public measures:** 91% → 74% after this change. Affected fields listed in D-series findings.
3. **Access-modifier gap:** nine new measures are implicitly `public` with no `#(doc)`; the governance reviewer recommends explicit annotation (G1–G9).

## Top Issues (Blockers & Criticals)

<Each entry uses this layout. List all blockers first, then all criticals. No majors/minors here, those go in Detailed Findings.>

- **B1** `issue (blocking, correctness-join)` · `models/finance/revenue.malloy:142` · conf 95
  - **Rule:** Declared `primary_key:` is not unique in the data
  - **Current:** `join_one: customers on customer_id = customers.id`; the `customers.id` column has duplicates (verified: `pk_verified=false` from SKILL.md step 3).
  - **Why it matters:** Malloy's symmetric-aggregation SQL relies on the declared PK being unique. When it isn't, the `DISTINCT` step collapses what shouldn't and aggregations across this join return hash-collision-sized garbage (~10²¹).
  - **Suggested fix:** Pick a different / composite PK that IS unique, OR add a source-level `where:` that scopes `customers` to a uniquely-keyed subset, OR declare a `#(filter) ... required` annotation so consumers must supply the discriminating filter.
  - **Source:** rubric-correctness.md (rule C-12); pk_verified=false from SKILL.md step 3.

## Detailed Findings by File

<Collapsed per file. Within each file, group by dimension. Each finding uses the short form below.>

<details>
<summary><code>models/finance/revenue.malloy</code>, 4 findings (1 blocker, 2 major, 1 minor)</summary>

- **B1** (correctness-join, conf 95) line 142, see Top Issues
- **D3** (docs major, conf 88) line 67, `#(doc)` missing on public measure `monthly_revenue`. Fix: add a one-line business description above the measure.
- **R2** (rendering major, conf 95) line 89, Fixed scale (`# currency=usd0m`) on measure definition. Fix: move the scale to the view and leave `# currency` on the definition.
- **D4** (docs minor, conf 82) line 71, `#(doc)` uses Malloy jargon ("aggregation of total revenue"). Fix: describe business meaning and units, e.g. `#(doc) Total revenue from completed orders in USD`.

</details>

<details>
<summary><code>models/shared/_joins.malloy</code>, 3 findings</summary>
...
</details>

## Questions for the Author
<Understanding gaps, things the review couldn't resolve without more context. One per bullet, each anchored to a file/line.>

- `models/finance/revenue.malloy:142`, is `customers.id` actually unique per customer? The CRM import documented in `modeling-notes.md` suggests duplicates on re-runs.
- `models/shared/users.malloy:12`, new source-level `where: project_id = @session.project` scopes everything downstream. Was this intentional for all consumers, or did a specific view need the filter?

## Positive Notes
<Praise, what's working well. Not filler; only when real.>

- Consistent `#(doc)` coverage on the new `models/analytics/` source: 100% of public fields documented with business-language strings.
- The `extend` chain restructuring in `models/marketing/attrib.malloy` is a real readability win; cross-cutting rename is clean.

## Suggested Follow-ups (non-blocking)
<Aggregated minor/nit findings. Point at the Detailed Findings section for specifics.>

- 34 style nits aggregated, see S-series findings. Consider a follow-up PR or running `/malloy-review --apply` against those IDs.
- Three backticked-passthrough dimensions (Y-series), recommend aliasing to business names.

## Suggested Split (only if diff > 2000 LOC or >30 files)
<Present only when the scope justifies it. Group files by coherent change themes.>

This review covers 10,173 lines across 47 files. Only ~1,500 lines were deep-reviewed; the rest were sampled. A stacked PR sequence for next time:

1. **Schema & staging**: `models/staging/*.malloy` + `publisher.json` bumps (12 files, ~1,800 LOC)
2. **Shared joins and PK hygiene**: `models/shared/*.malloy` (4 files, ~400 LOC)
3. **Finance domain**: `models/finance/*.malloy` (9 files, ~1,900 LOC)
4. **Marketing domain**: `models/marketing/*.malloy` (11 files, ~2,300 LOC)
5. **Governance & access annotations**: access-modifier pass across previously-shipped models (8 files, ~900 LOC)
6. **Consumer views and dashboards**: `views/*.malloy` (3 files, ~2,800 LOC)

## Machine-readable findings

<!-- Stable schema; safe for downstream tools and a future --apply mode to parse. -->

```json
{
  "schema_version": 1,
  "scope": {
    "paths": ["models/finance/", "models/shared/_joins.malloy"],
    "packages": ["finance"],
    "files_reviewed": 14,
    "total_loc": 2912
  },
  "diagnostics_coverage": {
    "available": 9,
    "missing": 5,
    "missing_files": ["models/finance/forecast.malloy", "..."]
  },
  "source_index": {
    "customers": {"file": "models/shared/customers.malloy", "primary_key": "id", "pk_verified": false, "wave": 1},
    "orders":    {"file": "models/orders.malloy",            "primary_key": "id", "pk_verified": true,  "wave": 1}
  },
  "findings": [
    {
      "id": "B1",
      "severity": "critical",
      "category": "correctness-join",
      "blocking": "blocking",
      "file": "models/finance/revenue.malloy",
      "line_range": [142, 142],
      "rule": "C-12 declared primary_key is not unique in the data",
      "current": "join_one: customers on customer_id = customers.id; customers.id has duplicates (pk_verified=false)",
      "expected": "customers.id is unique per row, or the customers source carries a where: that scopes to a uniquely-keyed subset",
      "suggested_fix": "Aggregations across this join will silently return hash-collision-sized garbage. Pick a different / composite PK that IS unique, OR add a source-level where: that makes id unique within the filtered set, OR declare a #(filter) ... required annotation.",
      "confidence": 95,
      "evidence": "rubric-correctness C-12; pk_verified=false from SKILL.md step 3 malloy_executeQuery check",
      "source": "rule"
    }
  ],
  "coverage_map": [
    {"file": "models/finance/revenue.malloy", "loc": 412, "risk": "HIGH", "depth": "DEEP"}
  ]
}
```
```

## Assembly rules

1. **Sections are always emitted in this order.** Skip a section if empty, but don't reorder.
2. **Top Issues caps at 25 inline entries.** If there are more blockers/criticals, list the first 25 and add a closing line: "...N additional blockers/criticals in Detailed Findings." This prevents API throttling on giant reviews.
3. **Detailed Findings is always collapsed with `<details>` tags**, users click to expand only the files they care about.
4. **JSON tail schema is stable (`schema_version: 1`).** Any future change to the shape bumps the version. The v2 `--apply` mode reads this.
5. **Diagnostics-coverage note is always present** even if no diagnostics were available. Write "Diagnostics unavailable on this host; open the repo in an editor with the Malloy extension for compiler-grade findings."
6. **"Suggested Split" section only appears** when the review scope is >2000 LOC or >30 files. Smaller reviews skip it entirely.
7. **"Positive Notes" must be real**, don't fill it with generic praise. If nothing deserves praise, omit the section.
8. **"Questions for the Author" is bounded to ≤5 bullets.** More than that means the summarizer pass didn't do its job; tighten that first, don't expand this.
9. **File paths are always repo-relative**, never absolute paths, never `~/`-prefixed.

## What the file does NOT contain

- No per-agent breakouts ("structural reviewer said X, docs reviewer said Y"). The reader doesn't care which agent found something.
- No rubric rule text in full, rubric references are by ID only (e.g., "rubric-correctness C-12"). The file cites where to look, it doesn't duplicate the rubric.
- No debug traces, no token counts, no model names.
- No raw diff. The reader has git for that.
