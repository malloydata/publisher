# Rubric: Rendering

**Dimension:** `rendering`
**Rules:** 9

Applies to any `.malloy` file with `view:` definitions or `#`-tagged fields. If the file has no rendering tags, skip this dimension.

For every rule, the linked instruction-skill section is the canonical source for rationale, valid values, and WRONG/RIGHT examples.

**A note on silent failure.** Malloy **silently ignores unknown rendering tags**, there's no compile error, no warning at runtime, no rendered chart, just a fallback to the default table view. R-09 below catches this. Run `mcp__ide__getDiagnostics` after editing chart tags; some malformed nested-tag syntax (e.g., `viz.stack.y` from `# bar_chart.stack { y=[...] }`) does surface as a renderer lint warning, which is strong evidence when present.

---

## R-01: Don't bake a fixed display scale into a measure definition

- **Severity:** minor (non-blocking) · **Category:** rendering · machine-checkable
- **Why this is a problem.** A scale like `# currency=usd1m` (display in millions) is correct for one specific value range, but a measure can be reused across many views. As soon as a view filters down, one customer, one month, one product, one region, the rendered values may fall well below the baked-in scale and round visibly: `$500` displays as `$0.0M`, `$1,200,000` collapses to `$1M` and loses precision. The scale belongs on the view (after you've seen the actual range), not on the measure definition where it can't anticipate every downstream filter.
- **Detection:** any explicit value on a `# currency`, `# number`, or `# percent` tag immediately above a `measure:` or `dimension:` declaration, e.g. `# currency=usd1m`, `# number=0,000`, `# percent=0.00`. The bare forms (`# currency`, `# number`, `# percent`) are correct and must not be flagged.
- **Fix:** drop the `=<scale>` from the measure or dimension. Add it at the view level for the specific views where the value range is known to fit.
- **See:** `skill:gotchas-rendering` § No Fixed Scale on Measures

---

## R-02: `# big_value` measures should have `# label`

- **Severity:** minor (non-blocking) · **Category:** rendering · machine-checkable
- **Detection:** AST, a view with `# big_value` tag whose `aggregate:` declarations lack a `# label="..."` tag.
- **Fix:** add `# label="..."` above each aggregate.
- **Why non-blocking:** verified against Malloy 0.0.370, the view compiles fine and the card still renders without `# label`. Malloy falls back to the raw field name on the card (`total_revenue` instead of `Total Revenue`), which is a clarity hit but not a render failure. Treat it as a polish recommendation, not a merge gate.
- **See:** `skill:gotchas-rendering` § `# big_value` Needs `# label` on Each Measure

---

## R-03: Sparklines on `# big_value` need both the `sparkline=` reference and a matching `# hidden` nested view

- **Severity:** major (blocking) · **Category:** rendering · machine-checkable
- **Detection:** AST, `sparkline=` is a `# big_value` property; sparklines aren't a generic chart feature, and `sparkline=` on any other view tag is silently ignored by the renderer. For any view tagged `# big_value` with `sparkline=<name>`, verify the view contains a `nest:` whose alias matches `<name>` and whose tag block carries both a chart tag (e.g. `# line_chart`, `# bar_chart`) and `# hidden`.
- **Fix:** put `# hidden` on the nested chart view, and make sure the nested view's name matches the `sparkline=<name>` reference on the parent.
- **See:** `skill:gotchas-rendering` § Sparkline Setup · `skill:malloy-charts` § Sparklines in KPI Cards

---

## R-04: `# big_value` comparison deltas need `comparison_label` to pair with `comparison_field`

- **Severity:** minor (non-blocking) · **Category:** rendering · machine-checkable
- **Detection:** regex, `comparison_field` and `comparison_label` are `# big_value` properties (along with `down_is_good` and the sparkline pair from R-03); they aren't a generic chart feature. For any view tagged `# big_value` whose tag block contains a `comparison_field=` property, verify a `comparison_label=` is also present in the same tag block (multi-line braced form counts).
- **Fix:** add `comparison_label="..."` with a human-readable description (e.g., `"vs Last Month"`).
- **See:** `skill:gotchas-rendering` § Comparison Deltas · `skill:malloy-charts` § KPIs with Comparison Deltas

---

## R-05: Currency codes and scale values must be valid

- **Severity:** major (non-blocking) · **Category:** rendering · machine-checkable
- **Detection:** regex the full `# currency=<value>` or `# number=<value>` string and validate the shape. Valid currencies: `usd`, `eur`, `gbp`, `jpy`, `cad`, `aud`, `chf`, `cny`, `inr`. Valid scale suffixes: `K`, `M`, `B`, `T`, `Q`, or `auto`. Decimals: integer 0–6 before the suffix.
- **Fix:** swap to a valid code/scale
- **See:** `skill:malloy-charts` § Field Formatting Tags

---

## R-06: `# number=id` for integer columns that aren't quantities (years, IDs, zip codes, phone numbers)

- **Severity:** major (non-blocking) · **Category:** rendering · machine-checkable (data-driven, falls back to LLM-judgment)
- **Why this matters.** Malloy's default integer formatting adds thousand-separator commas. A year displays as `2,018` instead of `2018`, a zip code as `94,107` instead of `94107`, an account number as `1,234,567` instead of `1234567`. The number is *visibly wrong* on every chart and table until `# number=id` strips the formatting. Especially common with `year(ts)` extraction (`skill:gotchas-queries` § Time Truncation vs Extraction) and with any FK/PK column that surfaces on a chart axis or in a result table.
- **Detection, preferred (data-driven).** For every integer dimension in scope, run `malloy_executeQuery`:

  ```malloy
  run: <source> -> {
    aggregate:
      rows is count()
      distinct is count(<col>)
      min_v is min(<col>)
      max_v is max(<col>)
  }
  ```

  Decide per column:
  - **Distinct-count ≈ row count** → per-row identifier (account/order/user/phone numbers). Should have `# number=id`.
  - **Year-shaped** (small distinct set, values in `1900–2100`) or `year(ts)` extraction → identifier-flavored, should have `# number=id`. Same for zip-shaped (small set of length-5 numeric), phone-shaped (length-10/11 numeric), categorical-numeric codes.
  - **Quantity-flavored** (values aggregate naturally, name suggests `_count` / `_total` / `_amount` / `_price` / `_quantity` / `_revenue`) → leave default formatting.
- **Detection, fallback (no `malloy_executeQuery`).** Name + type heuristics, noted in the finding so the reviewer can confirm:
  - Should-have-`# number=id` signals: name ends in `_id`, `_year`, `_zip` / `_zipcode`, `_phone`; column is a `year(...)` extraction; integer type with no arithmetic in any `aggregate:` referencing it.
  - Should-NOT-have-`# number=id` signals: name ends in `_count` / `_total` / `_amount` / `_price` / `_quantity` / `_revenue` / `_cost`.
- **Fix:** add `# number=id` above the dimension or measure.
- **See:** `skill:malloy-charts` § Field Formatting Tags · `skill:gotchas-queries` § Time Truncation vs Extraction (year-as-int discussion)

---

## R-08: Don't use `# hidden` as a model-visibility mechanism

- **Severity:** nit (non-blocking) · **Category:** rendering · LLM-judgment
- **What `# hidden` is.** A Malloy *renderer* tag, cosmetic output suppression. Legitimately used inside `# dashboard` and `# big_value` views: hiding the `nest:` that only exists to feed a sparkline, hiding the `prior_month` aggregate that backs a comparison delta, hiding a tile from a dashboard layout. It does NOT remove a field from the source's API surface, the field is still queryable, still appears in source metadata, still a valid `group_by` / `aggregate` reference. See `skill:malloy-charts` § Utility Tags.
- **The mistake this rule catches.** `# hidden` placed on a `dimension:` or `measure:` declaration inside a source's `extend {}` block (not inside a view), used as if it were an access modifier. It isn't. If the goal is "remove from the source's public API," the right tools are `internal:` (or `private:` for sensitive data) in an `include {}` block, see `malloy-model/reference/access-modifiers.md`. The lookml-review skill calls this out explicitly: "LookML `hidden: yes` ≈ Malloy `# hidden` (cosmetic). LookML `fields` exclusion ≈ Malloy `internal:` (structural). Do NOT map `hidden: yes` directly to `internal:`."
- **Detection (LLM-judgment).** Flag `# hidden` on a `dimension:` or `measure:` inside a source's `extend {}` block. `# hidden` *inside a view*, on a `nest:` for sparklines, on a comparison aggregate inside `# big_value`, on a tile in `# dashboard`, is correct and must not be flagged.
- **Fix:** classify intent first.
  - **"Remove from the API surface"** → move the column to `include { internal: <col> }` (or `private:` for sensitive) and drop the `# hidden`.
  - **"Suppress in a specific view's rendered output"** → drop `# hidden` from the `extend {}` declaration and re-add it inside the specific view that needs the suppression.
- **See:** `skill:malloy-charts` § Utility Tags (canonical `# hidden` definition) · `malloy-model/reference/access-modifiers.md` (for the `internal:` / `private:` alternative)

---

## R-09: Chart-rendering tags must be on the renderer's supported list

- **Severity:** major (blocking) · **Category:** rendering · machine-checkable
- **Why it matters.** The Malloy *core compiler* accepts any `# <tag>` annotation, verified against Malloy 0.0.370, where `# stacked_area_chart`, `# pie_chart`, and `# heatmap` all compile cleanly with no errors or warnings. Whether the renderer recognises the tag or silently drops it (rendering a flat table where the author expected a chart) is decided downstream in `@malloydata/malloy-render`. A real case: a view tagged `# stacked_area_chart` looked right in the code and rendered as a single-bar chart at runtime.
- **Detection, preferred (consume IDE diagnostics).** Recent versions of the Malloy renderer surface unknown render tags as `"Unknown render tag"` warnings with source-located info. When the host's renderer is on a recent-enough version *and* the IDE bridges renderer warnings into `mcp__ide__getDiagnostics`, the diagnostic pre-pass (SKILL.md step 2) is the right detection path, the rubric should NOT re-emit. Older renderers and hosts that don't bridge renderer warnings into diagnostics still need the explicit check below.
- **Detection, fallback (no diagnostics).** Flag every `# <tag>` annotation preceding a `view:` declaration where the tag isn't on the renderer's current allowlist. **Do not trust the rubric's hard-coded list as canonical**, call `malloy_searchDocs` for "chart types" before flagging, since the supported set evolves (the renderer-validation contract is one such evolution). As a baseline, the long-stable set is `bar_chart | line_chart | scatter_chart | shape_map | segment_map | big_value | dashboard | list | list_detail`; treat anything outside that set as a candidate finding only after `malloy_searchDocs` confirms it isn't a newly-added supported type.
- **Fix template (still useful regardless of detection path):**
  - For "stacked area / stacked column" intent → `# bar_chart { stack y=['col1', 'col2', 'col3'] }` (see R-10 for the exact syntax).
  - For "pie / donut" intent → not supported; redesign the view as a bar chart or use a custom renderer.
  - For "heatmap" intent → not supported natively; use `shape_map` / `segment_map` if geographic, otherwise present as a 2D-pivot table.
  - When `malloy_searchDocs` reveals a newly-supported type that fits the intent, prefer that over the workaround.
- **See:** `skill:malloy-charts` § Chart Types

---

## R-10: Multi-series chart syntax: flat properties + quoted strings

- **Severity:** major (blocking) · **Category:** rendering · machine-checkable
- **Detection:** when a chart tag attempts multi-series rendering via a `y=[...]` array, two specific bugs are common and BOTH produce silent partial rendering (only the first series shows up):
  1. **Nested-tag form**, `# bar_chart.stack { y=[...] }` causes the renderer to parse `.stack` as a nested tag path and reject `y` as an unknown sub-property. The IDE lint reports `Unknown render tag 'viz.stack.y' on field 'root'`, that warning is strong evidence when present.
  2. **Bare identifiers in the array**, `y=[col1, col2]` (no quotes) parses as something other than a string list and renders only the first measure. Quoted strings are required: `y=['col1', 'col2']`.
- **Fix template:**
  - Replace `# bar_chart.stack { y=[...] }` → `# bar_chart { stack y=['col1', 'col2'] }` (stack as a sibling property).
  - Replace `y=[col1, col2, ...]` → `y=['col1', 'col2', ...]` (quoted strings).
- **Why it matters:** the Malloy upstream docs' example uses the nested-tag form (`# bar_chart.stack { y=[...] }`), which is misleading, the renderer expects the flat form. Both forms are documented, but only one parses correctly in the actual renderer.
- **See:** `skill:malloy-charts` § Multi-Series Bar Charts · `skill:malloy-charts` § Stacked Bar Charts
