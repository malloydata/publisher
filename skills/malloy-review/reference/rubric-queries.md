# Rubric: Queries & Views

**Dimension:** `queries`
**Rules:** 4 (focused on shape and judgment, not parse errors)

Applies to in-scope `.malloy` files containing `view:` or `run:` definitions. If a file has no views or runnable queries, skip this rubric for that file.

Compile-caught query mistakes, trailing commas between clauses, `having:` at source level, unaliased dotted paths in `order_by:`, window functions outside `calculate:`, `nest:` group-by fields that don't exist on the parent source, `where:` on a name that resolves to an aggregate, are all surfaced by the IDE diagnostic pre-pass with clear messages. The reviewer must not re-derive them.

What this rubric covers is the **shape and intent** of views: chart-rendering choices, reusability, time-axis judgment, and exploratory ergonomics. These survive compilation but make queries less useful or harder to maintain.

For every rule, the linked instruction-skill section is the canonical source for rationale and WRONG/RIGHT examples.

---

## Q-01: Charts render one aggregate only

- **Severity:** major (non-blocking) · **Category:** queries · machine-checkable
- **Detection:** within a view preceded by a chart-rendering tag (`# bar_chart`, `# line_chart`, `# scatter_chart`, `# shape_map`, `# segment_map`), count `aggregate:` lines AND `group_by:` lines.
  - **Bug shape:** chart tag + ≥2 aggregates + ≤1 group_by + no `y=[...]` spec → flag. Only the first aggregate renders; the rest are silently dropped.
  - **Already correct (no flag):** chart tag + 1 aggregate + ≥2 group_bys → the second group_by drives the chart series (e.g. `time × category`).
  - **Already correct (no flag):** chart tag + `y=['col1', 'col2']` on the tag → multi-series rendering. Verify the `y=[...]` syntax per `rubric-rendering.md` § R-10 (must be flat properties + quoted strings).
- **Fix template, three patterns based on the view's shape:**
  1. **Multi-series via `y=[...]`**, when 2 aggregates share a Y-axis scale (both counts, or both currencies). Example: `# line_chart { y=['new_customers', 'returning_customers'] }` over `group_by: order_year_month`. Note the QUOTED STRINGS in the array; bare identifiers silently render only the first measure.
  2. **Strip the chart tag**, when 3+ aggregates, or when 2 aggregates have different units (currency + count + rate). Leave the view as a data table; consumers chart whichever metric they care about. This is the established pattern in mart-derived files where one view exposes ~10 KPIs.
  3. **Keep single aggregate + 2 group_bys**, when there's a natural category dimension to drive the series. Drop the extra aggregates from the chart-tagged view; if needed, create separate views per metric.
- **See:** `skill:gotchas-queries` § Charts, ONE Aggregate Per View · `skill:malloy-charts` § Multi-Series Charts · `rubric-rendering.md` § R-10 (multi-series syntax gotchas)

---

## Q-02: Define measures in the source, not inline in views (DRY)

- **Severity:** minor (non-blocking) · **Category:** queries · LLM-judgment, but **promote to `major (blocking)` when the inline measure is `avg(<per_row_rate_dimension>)`** (see "high-priority sub-pattern" below).
- **Detection:** AST: flag `aggregate: <name> is <expression>` inside a view where the expression is non-trivial (more than a column reference). LLM judgment on whether the expression is reusable.
- **Fix:** move the measure definition into the source; reference by name in the view.
- **See:** `skill:gotchas-queries` § DRY, Define in Source, Reference in View

**High-priority sub-pattern, inline `avg(per_row_rate)` IS the avg-of-rate anti-pattern.**

When a source defines a row-level ratio as a dimension (often with a docstring like "Row-level ratio for averaging in views" or "per-row CM3 ratio") AND views use `aggregate: avg_X is avg(rate_dimension)`, you have **two converging bugs in one place**:

1. The inline measure is a Q-02 DRY violation.
2. `avg(per_row_rate)` is the avg-of-rate anti-pattern: it gives every row equal weight regardless of underlying volume, dramatically overstating or distorting performance metrics.

**Detection heuristic:** when you see `aggregate: avg_X is avg(Y)` inside a view body, check if `Y` is defined as a `dimension:` on the source AND whether that dimension is itself a ratio (`A / nullif(B, 0)`). If so, this is the high-priority sub-pattern.

**Fix template:**

1. Check whether the source ALREADY has a volume-weighted measure that does the math correctly: `measure: cm3_percentage is attributed_cm3 / nullif(attributed_revenue, 0)`. If yes, replace each inline `avg_X is avg(rate)` with `avg_X is cm3_percentage` (or whatever the correct source measure is). This preserves the public column name `avg_X` for backward compatibility while fixing the math.
2. If no source-level measure exists, define one: `measure: X_weighted is sum_of_numerator / nullif(sum_of_denominator, 0)`.
3. Mark the row-level ratio dimensions `# (doc) DEPRECATED, per-row ratio, do not average. Use X_weighted instead.` so future readers don't re-introduce the pattern.

**Magnitude check:** switching `avg_roas` from `avg(roas_value)` to volume-weighted `roas` (`attributed_revenue / attributed_marketing_spend`) corrected a **4.5× overstatement** of marketing ROI (0.289 reported vs 0.063 actual) in a real model. Inline avg-of-rate is rarely off by less than 10%; the rule is worth promoting to `blocking` once you see the pattern.

---

## Q-03: Time truncation vs extraction, pick the right one for the context

- **Severity:** major (non-blocking) · **Category:** correctness-type · LLM-judgment
- **Detection:** when `month()` / `year()` / `day_of_week()` is used in a `group_by:` whose view is a time-series chart, suggest the truncation form (`.month`, etc.). The reverse (using `.year` where integer extraction was wanted) is rare but possible.
- **Fix:** swap the form; for year integers add `# number=id` to suppress comma formatting (R-06)
- **See:** `skill:gotchas-queries` § Time Truncation vs Extraction

---

## Q-04: Cap `limit:` where it matters

- **Severity:** nit (non-blocking) · **Category:** queries · machine-checkable
- **Detection:** regex: a `run:` or top-level `query:` with an `order_by: ... desc` clause and no `limit:`. Only flag in exploratory contexts, scheduled jobs or full extracts may intentionally omit.
- **Fix:** add a reasonable `limit:` (20/50/100); suggest the number, let the user tune
- **See:** weak counterpart in instruction skills, exploratory ergonomics convention rather than a taught principle

---

## Detection guidance for the queries reviewer

1. **Consume diagnostics first.** Trailing commas, `having:` at source level, unaliased dotted paths in `order_by:`, window functions outside `calculate:`, missing fields in `nest:` group-bys, `where:` on aggregate names, all parse/compile errors handled by the IDE pre-pass. Do not re-derive.
2. **Focus on shape, not syntax.** Q-01 is about chart-rendering behavior; Q-02 is about reuse; Q-03 is about which time form fits the consuming view; Q-04 is about result-set ergonomics. None of these fail at compile time.
