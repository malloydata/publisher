# Rubric: Correctness (Semantic Hazards Only)

**Dimension:** `correctness-join | correctness-aggregation | correctness-type | correctness-filter`
**Rules:** 9 (focused on silently-wrong behavior, not parse errors)

This rubric only contains rules that the Malloy compiler does NOT catch. Parse errors, missing colons, `as` vs `is`, missing `->`, bare `join:`, `||` concat, unbacked reserved words, redefined query-source columns are all caught by the IDE diagnostic pre-pass (when available, via `mcp__ide__getDiagnostics`) and surface as `source: "diagnostic"` findings, the reviewer must not re-derive them.

What this rubric covers is the dangerous middle ground: code that **compiles cleanly but is wrong**, silent cardinality bugs, division-by-zero, string aggregates, raw SQL bypassing Malloy's guarantees, declared primary keys that aren't actually unique in the data.

**Rules we dropped after compiler-source / live-compile validation:**

- **C-07 (missing `primary_key:` on a `join_one:` target).** Reading the Malloy compiler source (`packages/malloy/src/model/field_instance.ts:644-670`, `query_query.ts:946-951`) shows the compiler auto-synthesizes a UUID-based `__distinct_key` when the target lacks a declared PK, so symmetric aggregation is correct either way. The `with` shortcut (which DOES require a declared PK) is enforced by the compiler with a clear error and surfaces as a diagnostic. The remaining concern, "declared PK is a lie", is what C-12 below catches. The hygiene preference (declare a PK when one exists) lives in `rubric-structure.md` § S-02 as a `minor` recommendation; the style preference for using `with` consistently across the project lives in `rubric-style.md` § Y-03.
- **C-08 (use method syntax for joined-field aggregation, e.g. `sum(items.cost)` → `items.cost.sum()`).** Confirmed against Malloy 0.0.370: `sum(joined.field)`, `avg(joined.field)`, `min(joined.field)`, and `max(joined.field)` are all **compile errors** with the message `Join path is required for this calculation; use 'items.cost.sum()'` (the diagnostic literally suggests the fix). The IDE diagnostic pre-pass surfaces this with a clearer message than the rubric ever could. The `count(joined.field)` carve-out, that it remains the canonical Malloy distinct-count idiom and should not be "fixed" to method syntax, is documented in `skill:gotchas-queries` § Aggregating Joined Fields, Method Syntax, which is where someone would land after seeing the compiler nudge.
- **C-11 (`?` alternation joined by `and` rather than comma).** Validated against Malloy 0.0.370 across five arrangements. The compiler is never silently wrong here: `is_us = true and party ? 'D' | 'R'` (alternation second) compiles to the SQL the author intended (`WHERE is_us=true AND party IN ('D','R')`); `party ? 'D' | 'R' and is_us = true` (alternation first) and any pair of `?` alternations joined by `and` produce a clean compile error (`'logical operator' Can't use type string`). Either path is reviewer-safe, so the rubric doesn't need to flag it. Comma is still the canonical form because it's unambiguous in every position; that guidance lives in `skill:gotchas-queries` § `?` Alternation, Use Commas to Combine Filters.

For every rule, the linked instruction-skill section is the canonical source for rationale and WRONG/RIGHT examples, read it once, don't paraphrase here.

---

## C-01: NULL checks: prefer `is not null` over `!= null`

- **Severity:** major (non-blocking) · **Category:** correctness-filter · machine-checkable
- **Detection:** regex `!=\s*null` or `=\s*null` in a `where:` clause or boolean-dimension expression. (Malloy uses `=` for equality, not `==`.)
- **Fix:** `X is not null` or `X is null`
- **Why non-blocking, Malloy itself recommends the form we're flagging.** The Malloy compiler emits a *warning* suggesting `!= null` over `is not null` ([`malloydata/malloy#1880`](https://github.com/malloydata/malloy/pull/1880)). Both forms compile. The reason this rubric still prefers `is not null` is that for the canonical boolean-dimension pattern `dimension: is_X is column != null`, Malloy evaluates `null != null` as `true` (bug [`malloydata/malloy#1968`](https://github.com/malloydata/malloy/issues/1968); fix proposal [`#2067`](https://github.com/malloydata/malloy/pull/2067) closed unmerged in Jan 2025), so the dimension marks null rows as `true` when the author intended `false`. Until that's resolved, `is not null` is the safer form for column-vs-null checks, even though the IDE will soft-warn the other way. Reviewers should weight this contradiction when assigning per-finding confidence.
- **See:** `skill:gotchas-modeling` § NULL Checks, `is not null`, NOT `!= null`

---

## C-02: Date/time return-type confusion (`.month` vs `month()`)

- **Severity:** major (non-blocking) · **Category:** correctness-type · LLM-judgment
- **What this catches.** Malloy has two forms for date parts. Both compile cleanly, but they return different types:

  | Syntax | Returns | Use for |
  |---|---|---|
  | `ts.year` / `ts.month` / `ts.quarter` / `ts.day` / `ts.week` | **Timestamp** truncated to that boundary (e.g. `@2024-03-01`) | Time-series chart axes, ordered grouping, range filters |
  | `year(ts)` / `month(ts)` / `quarter(ts)` / `day(ts)` / `hour(ts)` | **Integer** (e.g. `2024`, `3`) | Cross-year buckets ("all Januaries together"), `pick when` integer comparisons, integer arithmetic |

  The wrong choice is silent, there's no compile error. Concrete hazards:
  - Chart view (`# bar_chart` / `# line_chart`) using `group_by: m is month(ts)` renders integers as `1, 10, 11, 12, 2, 3, …` (lexicographic order) instead of chronological months.
  - `where: created_at.year > 2020` compares a timestamp (`@2020-01-01`) to integer `2020` and doesn't filter what the author intended.
  - Cross-year "January totals" using `ts.month` produces 12 buckets *per year* (e.g. 60 buckets across 5 years) instead of 12 buckets total. The chart looks chronologically normal so the bug is easy to miss.
- **Detection (context-dependent, LLM-judgment):**
  - Chart view on a time axis using `<fn>(ts)` (integer form) → usually wants `ts.<part>` (timestamp form).
  - `where:` / `pick when` / numeric comparison against an integer literal using `ts.<part>` (timestamp form) → usually wants `<fn>(ts)` (integer form).
  - Aggregation labelled "monthly" / "by month" grouping on `ts.month` across multiple years → the actual bucket key is *month-and-year*, not *month-of-year*; the author probably meant `month(ts)`.
- **Fix.** Pick the form the caller's context expects, and rename the dimension so the type is unambiguous in downstream code:
  - Timestamp form → `<part>_at` / `<part>_truncated_at` (e.g. `month_truncated_at is created_at.month`).
  - Integer form → `<part>_number` / bare `<part>` (e.g. `month_number is month(created_at)`).
- **Not a finding (the compiler already catches these).** Using `.day_of_week`, `.hour`, `.minute`, `.second`, or `.week` on a timestamp errors at compile time, those date parts are *functions only*, not properties. The IDE diagnostic pre-pass surfaces them; don't re-emit as C-02.
- **See:** `skill:gotchas-modeling` § Date Functions vs Properties · `skill:gotchas-queries` § Time Truncation vs Extraction

---

## C-03: Safe division with `nullif`

- **Severity:** major (blocking) · **Category:** correctness-filter · machine-checkable
- **Detection:** regex for `/` in measure/dimension expressions where the right-hand side is not a `nullif(...)` call
- **Fix:** `<numerator> / nullif(<denominator>, 0)`
- **See:** `skill:gotchas-modeling` § Safe Division, Always `nullif`

---

## C-04: Cast strings for aggregates

- **Severity:** major (blocking) · **Category:** correctness-type · LLM-judgment
- **Detection:** when an aggregate (`avg`, `sum`, etc.) wraps a column whose type is `STRING`, flag. To learn a column's type, read the model with `malloy_modelGetText` or `malloy_packageGet` (the model defines the sources and their fields), or sample the data with `malloy_executeQuery` (`run: <source> -> { select: * limit: 1 }`).
- **Fix:** `avg(score::number)` / `sum(amount::number)`
- **See:** `skill:gotchas-modeling` § String Columns Need Casts for Aggregates

---

## C-05: Boolean comparisons use bare `true`/`false`

- **Severity:** major (blocking) · **Category:** correctness-type · machine-checkable
- **Detection:** regex for `=\s*['"]true['"]` or `=\s*['"]false['"]`
- **Fix:** remove the quotes
- **See:** `skill:gotchas-modeling` § Boolean Columns, No Quotes

---

## C-06: Don't combine `rename:` with `include {}` access-modifier blocks

- **Severity:** major (blocking) · **Category:** correctness-syntax · machine-checkable (conditional)
- **Detection:** find every `rename:` occurrence; flag ONLY when the same `source:` block also contains an `include {…}` clause. A `rename:` in a source without `include {}` is fine and must not be flagged.
- **Why this is wrong:** `include {}` and `rename:` don't compose. `rename:` runs first at parse time, so by the time `include` tries to attach an access modifier the original column name is gone, Malloy errors `Can't find field 'X' to set access modifier`.
- **Fix template, `include {}` is the curated default; only drop it when `rename:` is unavoidable:**
  - **Preferred, preserve `include {}` (the curated default).** Eliminate the `rename:` so `include {}` can stay. Most often this means renaming the colliding *measure* (e.g., `measure: revenue` → `measure: total_revenue`) or splitting the source into a base + computed source. `include {}` is the canonical curated mode for documented base sources, it's the only way to attach `#(doc)` tags to raw columns and the recommended way to hide empty/garbage/duplicate columns via `internal:`. See `malloy-model/reference/access-modifiers.md`. Keep it whenever you can.
  - **Fallback, drop `include {}` when the rename is unavoidable.** Some renames can't be eliminated without a downstream-breaking change: most commonly `sql()` → `table()` migrations where the original SQL alias matches a measure name that's already in heavy use across notebooks/dashboards. In that case, drop `include {}` and curate the source with `extend { except: a, b, c }` + `rename: raw_X is X` instead. You forfeit `#(doc)` on raw columns and the `public/internal/private` tiers, but keep column gating and the rename.
- **Compile-error notes (validated):**
  - `include { ... } extend { rename: A is B }` errors with `Can't find field 'B' to set access modifier` because `rename:` runs first and leaves no `B` for `include` to set a modifier on.
  - `include { internal: X } extend { measure: X is ... }` errors with `Cannot redefine 'X'` because internal columns and measures share a namespace, Malloy disallows shadowing even for internal-tagged columns. The collision is what usually drives someone to add `rename:` in the first place.
- **See:** `skill:gotchas-modeling` § Field Management, `extend {}` vs `include {}` Don't Compose · `malloy-model/reference/access-modifiers.md` (for the `include {}` capability tradeoffs)

---

## C-09: `count()` vs `count(field)`

- **Severity:** major · **Category:** correctness-aggregation · LLM-judgment
- **Detection:** when a measure name suggests counting a specific entity (`unique_customers`, `distinct_products`) but uses `count()` on a source whose grain is something else, flag
- **Fix:** `count()` for rows; `count(x)` for distinct values of x. Make the measure name match the grain.
- **See:** `skill:gotchas-modeling` (count semantics referenced throughout; weak counterpart, relies on reviewer judgment about measure-name-vs-grain mismatch)

---

## C-10: Use Malloy native patterns when available

- **Severity:** major (non-blocking) · **Category:** correctness-syntax · LLM-judgment
- **Detection:** find every `conn.sql(` occurrence, then **analyze the embedded SQL**:
  1. Read the SQL string.
  2. **MANDATORY pre-check, schema verification.** Use `malloy_executeQuery` to run `run: <table_path> -> { select: * limit: 1 }` against the underlying table referenced in the SQL. Compare the table's column list to the SQL's `SELECT` list. **Any column in the table that's NOT in the SELECT was being intentionally hidden by the SQL.** If so, the migration must preserve gating via `extend { except: ... }`, NOT just swap `sql()` → `table()`. Especially watch for columns named `month_*`, `weekly_*`, `daily_*`, `total_*`, `cum_*`, `running_*`, or anything matching `(month|day|week|hour)_<measure>`, these are typically pre-aggregated per-period rollups that would silently double-count if exposed and summed.
  3. Identify constructs used: `SELECT`, `GROUP BY`, `JOIN`, `CASE WHEN`, window functions (`LAG`, `LEAD`, `ROW_NUMBER`, `RANK`, `OVER`), CTEs, lateral joins, UNNEST, PIVOT, latest-snapshot subqueries, DB-specific functions, column aliases (`X AS Y`).
  4. **MANDATORY: call `malloy_searchDocs` for each non-trivial construct**, window functions, CTE chains, UNNEST/array access, PIVOT/conditional aggregation, latest-snapshot lookups. "When unsure" historically read as "never", agents skipped this step and asserted `conn.sql()` was justified when in fact Malloy expressed the pattern fine. The skip is not allowed. Record the search queries you ran inline in the finding's `suggested_fix` so the user can audit them.
  5. **If every construct has a Malloy equivalent** → flag, with a sketched rewrite that includes the preserved gating clause.
  6. **If any construct genuinely requires raw SQL** → don't flag, but the genuine-requirement list is narrow (DML/DDL, certain `MERGE` patterns). Do not accept "Malloy can't express this cleanly" without citing a specific `malloy_searchDocs` query that returned no equivalent.
- **Common mappings the agent should know:**
  - `SELECT col, AGG(...) FROM x GROUP BY col` → `x -> { group_by: col; aggregate: ... is ... }`
  - `SELECT explicit_cols FROM x` (column gating) → `x extend { except: <columns_not_in_SELECT> }` or `accept: <columns_in_SELECT>`
  - `SELECT col_x AS col_y FROM x` → `extend { rename: col_y is col_x }`
  - `SELECT CAST(ROUND(a * b) AS INT64) AS total FROM x` → `extend { dimension: total is round(a * b)::number }`
  - `WHERE created_at > '2024-01-01'` → `extend { where: created_at > @2024-01-01 }`
  - `LAG/LEAD/ROW_NUMBER/RANK ... OVER (...)` → `calculate: ... is lag(...)`, `rank()`, etc.
  - **`SUM(x) OVER (PARTITION BY ... ORDER BY ... ROWS UNBOUNDED PRECEDING)`** → `calculate: c is sum_cumulative(x) { partition_by: ...; order_by: ... asc }`
  - **`SUM(x) OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)`** (cumulative-excluding-current) → `sum_cumulative(x) - x` with the same `partition_by`/`order_by`
  - **Multi-CTE pipelines** → stacked query-based sources: `source: a is t -> {...}`; `source: b is a -> {...}`
  - **UNNEST / array access** → `array_column.each.field` ([data types docs](https://docs.malloydata.dev/documentation/language/datatypes#array-access))
  - **PIVOT / `SUM(CASE WHEN cat = 'a' THEN x END)`** → filtered aggregates: `aggregate: a is x.sum() { where: cat = 'a' }`
  - **`WHERE date = (SELECT max(date) FROM …)`** (latest snapshot) → `join_cross` to a one-row aggregate source, then filter on the joined `max_date` field
  - **Multi-key joins (`ON a.x = b.x AND a.y = b.y`)** → `join_one: b on x = b.x and y = b.y`
  - `CASE WHEN ... THEN ... ELSE ... END` → `pick ... when ... else ...`
  - `GREATEST(a, b)` / `LEAST(a, b)` → native: `greatest(a, b)` / `least(a, b)` (work as Malloy dimensions, no `conn.sql()` needed)
  - `COALESCE(a, b)` → `a ?? b`
  - `CAST(x AS y)` → `x::y`
  - Dialect-specific scalar functions Malloy doesn't model → `function_name!return_type(args)` (raw-SQL function escape; does NOT require `conn.sql()`)
- **The `rename:` chain pattern.** When the original SQL re-used column names via a swap (e.g., `SELECT cash_month AS payment_date, payment_date AS payment_day`), Malloy supports the same swap as two ordered `rename:` clauses: `rename: payment_day is payment_date` (frees the name), then `rename: payment_date is cash_month` (re-uses it). Malloy evaluates renames in source-block order. (`rename:` works inside `extend {}` alongside `except:` / `accept:` but **not** alongside `include {}`, see C-06.)
- **Fix:** propose the equivalent Malloy when expressible, **preserving the column gating** discovered in the pre-check step. When not expressible, the rule doesn't fire, leave `conn.sql()` alone.
- **Worked cautionary example:** A reviewer once tier-classified a 6-CTE 148-line `sql()` block (forecast pipeline with multi-key joins, latest-snapshot, custom-frame window function, conditional pivot, transfer-math `GREATEST/LEAST` chains) as "justified, Malloy can't express the window function with `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING` cleanly." They never called `malloy_searchDocs`. After actually searching, the equivalent is `sum_cumulative(x) - x { partition_by: ... }`, eight lines. The port came out 160 lines (12 more than the SQL), byte-identical across 2,160 cells of test data. The "Malloy can't do this" intuition was wrong, and the cost was duplicated review work plus several years of avoidable `conn.sql()` maintenance burden. **If you (the reviewer) catch yourself writing "this is awkward in Malloy" without having searched, stop and search.**
- **See:** `skill:gotchas-modeling` § Never Use `conn.sql()` When Malloy Has a Native Pattern (canonical equivalence table) · `skill:gotchas-modeling` § Field Management, `extend {}` vs `include {}` Don't Compose

---

## C-12: Declared `primary_key:` must actually be unique in the data

- **Severity:** critical (blocking) · **Category:** correctness-join · machine-checkable (data-driven)
- **Detection:** SKILL.md step 3 runs `malloy_executeQuery` for each source with a declared `primary_key:` and merges the result into `source_index.<src>.pk_verified` as `true | false | "skipped" | "error"`. **Emit C-12 only when `pk_verified == false`**, the source's declared PK has duplicates in the data. When `pk_verified` is `"skipped"` or `"error"`, do not emit; note the diagnostic-coverage gap in the output's Scope section instead.
- **Why this matters:** Malloy's symmetric-aggregation SQL relies on `HASH(pk) * 1e15` being unique per row. If the PK is a lie, the `DISTINCT` step collapses what shouldn't, and aggregations across `join_one:` to this source return hash-collision-sized garbage (~10²¹). The filter that "fixes" the symptom is doing so accidentally, by happening to produce a uniquely-keyed subset.
- **Fix template:** three paths:
  1. Pick a different (or composite) PK that *is* unique. For composite keys in Malloy, derive a single dimension that concatenates the key columns and use that as `primary_key:`.
  2. Add a source-level `where:` that makes the existing PK unique within the filtered set (acceptable when the filter is the source's intended scope).
  3. Declare a `#(filter) … required` annotation per the publisher filter mechanism so consumers must supply the discriminating filter (`skill:malloy-document` § #(filter) Tag, Declare Parameterizable Filters).
- **See:** `malloy-model/reference/bridge-tables.md` § Cardinality Verification · `skill:malloy-document` § #(filter) Tag, Declare Parameterizable Filters (for the `required` filter remediation path)

---

## Detection guidance for the correctness reviewer

1. **Consume diagnostics first**, SKILL.md step 2 already mapped IDE diagnostics to findings with `source: "diagnostic"`. Parse errors, syntax errors, missing colons, missing arrows, unbacked reserved words, redefined columns, all the compiler's job. Don't re-emit; don't add rubric rules for compile-caught problems.
2. **Focus on silently-wrong behavior.** Every rule here is a case where the code compiles but is semantically incorrect.
3. **Work from the rubric, not from intuition**, only emit findings that map to a rule ID here. If you see a semantic problem that doesn't match any rule, emit it as `rule: "judgment"` and explain it.
4. **Emit nothing if the rule doesn't fire.** The output should feel curated, not exhaustive.
