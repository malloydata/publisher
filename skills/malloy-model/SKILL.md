---
name: malloy-model
description: Build Malloy semantic models with base source and joined source files. Use when creating or modifying .malloy files, user asks to "create a malloy model", "add dimensions", "add measures", "create a source", or any Malloy model authoring task.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Building Malloy Models

> **Tool names** are written bare here - `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

## Getting Started (New Projects)

If no `.malloy` files exist yet, do discovery and propose a structure first, then return here to build base source and joined source files. Keep proposals and the analysis behind them in the conversation.

**File structure convention** (a flat layout at the package root is the simplest default):
```
<package-name>/
  publisher.json              # Required for publishing (name, version, description)
  customers.malloy            # Base source: one per table
  products.malloy
  orders.malloy
  user_order_facts.malloy     # Computed source
  order_analysis.malloy       # Source: one per analytical domain
  customer_health.malloy
```

Versions for new packages should start at "0.0.1".

**Before creating any files**, check for an existing `publisher.json` in the target directory. If one exists for a different package, create a new subdirectory for your package, don't overwrite another package's config.

In Publisher an environment is a project, and Publisher is single-tenant, so there is no org/tenant layer to model around: one environment holds one set of packages.

## Prior Art Dispatch

If your discovery turned up existing modeling patterns to mirror (a derived table, UNNEST joins, a review or curation pass), read the relevant reference before building.

| Pattern found in prior art | Reference to read |
|---------------------|-------------------|
| Derived table (PDT/NDT) | `skill:malloy-lookml-review` build-derived-tables guidance |
| UNNEST joins or struct access | `skill:malloy-lookml-review` build-unnest guidance |
| Review pass for coverage | `skill:malloy-lookml-review` review-coverage guidance |
| Curate pass with visibility seeds | `skill:malloy-lookml-review` curate-visibility guidance |

## Base Source Templates

### Base Source (Simple Mode)

```malloy
source: customers is my_conn.table('sales.customers')
extend {
  primary_key: customer_id

  dimension:
    // A dimension is the lighter way to give a column a cleaner name
    order_type is `Type`
    full_name is concat(first_name, ' ', last_name)
    segment is lifetime_value ?
      pick 'enterprise' when >= 100000
      pick 'mid-market' when >= 10000
      else 'SMB'

  measure:
    customer_count is count()
}
```

> **Every `dimension:` needs `name is expr`.** A bare column name like `dimension: species` is a parse error (e.g. `missing IS at ...`). Raw columns are already usable in `group_by` / `select` without any declaration, so only add a `dimension:` when deriving or renaming a field (e.g. `revenue is price * quantity`).

### Base Source (Curated Mode with Access Modifiers)

```malloy
##! experimental.access_modifiers

source: orders is my_conn.table('sales.orders')
include {
  public:
    #(doc) Order identifier
    order_id

    #(doc) Customer who placed the order
    user_id

    #(doc) Total sale price in USD
    sale_price

    #(doc) Order creation timestamp
    created_at

  internal:
    raw_payload_json  // Verified empty via index query + user confirmation
}
extend {
  primary_key: order_id

  dimension:
    #(doc) Date the order was placed
    order_date is created_at::date

  measure:
    #(doc) Total number of orders
    order_count is count()

    #(doc) Total revenue in USD
    # currency
    revenue is sum(sale_price)
}
```

### Computed Source (from Query)

```malloy
import "orders.malloy"

source: user_order_facts is from(
  orders -> {
    group_by: customer_id
    aggregate:
      total_orders is count()
      total_revenue is sum(sale_price)
      first_order_date is min(created_at)
      last_order_date is max(created_at)
  }
) extend {
  primary_key: customer_id

  dimension:
    days_since_last_order is days(last_order_date to now)
    is_repeat_buyer is total_orders > 1

  measure:
    buyer_count is count()
    avg_customer_ltv is avg(total_revenue)
}
```

For advanced query-based source patterns (window functions, pipelines), see `reference/query-sources.md`.

## Joined Source File Template

```malloy
import "customers.malloy"
import "orders.malloy"
import "user_order_facts.malloy"

#(doc) Customer health analysis. Use for retention, segmentation, and churn risk.
source: customer_health is customers extend {
  join_one: user_order_facts with customer_id
  join_many: orders on customer_id = orders.customer_id

  dimension:
    is_at_risk is user_order_facts.days_since_last_order > 90
      and user_order_facts.total_orders > 1

  measure:
    revenue_per_customer is orders.sale_price.sum() / nullif(customer_count, 0)
    at_risk_count is count() { where: is_at_risk = true }
}
```

## Base vs Joined Sources

| | Base Joined Source File | Joined Source File |
|---|---|---|
| **Contains** | One table's fields | Joins between base sources |
| **Dimensions** | Intrinsic to this table only | Cross-source (require joins) |
| **Measures** | Single-table aggregations | Cross-source aggregations |
| **Joins** | None (or only lookup joins intrinsic to the source) | Defines relationships between base sources |
| **Views** | None, in schema-first (see below) | None, in schema-first (see below) |
| **One per** | Physical table or computed source | Analytical domain |

**The "no views" rule is schema-first only.** A schema-first model is built before anyone
has asked a question, so any view in it is a guess. It does **not** apply to the
analysis-first workflow (`skill:malloy-model-as-you-go`), where every view is a
question that was asked and verified - there, **saving a view or a dashboard is the right
call**, in the model file next to the measures it uses. Analysis-first still models
everything else properly: documented dimensions, measures, and joins.

**Two more rules here are schema-first only.** Analysis-first should skip access modifiers
and curation - there is no discovery surface to curate when every field was paid for by a
question - and skip one-file-per-table, keeping a single domain file until it genuinely
gets unwieldy.

## Key Rules

- **Every `dimension:` needs `name is expr`**: a bare `dimension: species` is a parse error. Raw columns are queryable directly in `group_by` / `select`; only declare a `dimension:` to derive or rename a field.
- **Define joined tables before referencing them**, use `import` statements in multi-file architecture
- **Use `nullif(denominator, 0)` for all division**
- **Alias joined fields before using in `order_by`**: `group_by: yr is table.year`
- **Verify join paths** exist before referencing `a.b.field` (each hop needs explicit join)
- **Pick syntax**: value BEFORE condition, `pick 'Small' when size < 10`
- **`where:` vs `having:`**: Use `where:` for row filters, `having:` for aggregate filters
- **`rename:` composes with `include {}`, but only in one order**: the `extend { rename: }` must come before the `include {}`, which then names the field by its new name. Reversed, it fails with `Can't find field 'X' to set access modifier`. For a cleaner column name without a rename, `internal:` + `dimension:` is still the lighter move (mark `` `Type` `` as `internal`, add `` dimension: order_type is `Type` ``). See `skill:malloy-gotchas-modeling` § Field Management
- **Mark raw columns `internal` when a derived dimension replaces them**
- **Check for duplicate rows** before building measures
- When both a combined table (all types) and filtered/split tables exist, prefer the split tables
- **DRY: define measures/dimensions in base source files, not inline in views**
- **Never write a threshold, tier boundary, or bucket cutoff you chose yourself.** Every boundary in a `pick` expression or filtered measure is user-supplied, distribution-derived (query `min`/`p25`/`p50`/`p75`/`p95` first and show the evidence; see `skill:malloy-define` § Data-driven proposals), or explicitly flagged as an assumption in its `#(doc)`. A hardcoded cutoff nobody confirmed is a business decision shipped as fact.

## Parameterizing sources with `given:` (preferred)

Native Malloy **`given:` parameters** are the going-forward way to expose tunable knobs (date range, region, manufacturer) on a source - prefer them over `#(filter)` when you author a new model. A `given:` is a first-class runtime parameter you reference in the model's own logic; callers supply values at query time and the model uses them however it declares. Enable them with `##! experimental.givens` at the top of the model.

```malloy
##! experimental.givens

given:
  manufacturer_filter :: filter<string> is f''
  subject_filter :: filter<string> is f''

source: recalls is duckdb.table('data/auto_recalls.csv') extend {
  where: Manufacturer ~ $manufacturer_filter, Subject ~ $subject_filter
  measure: recall_count is count()
}
```

A given is **declared bare** but **referenced with a `$` sigil** in expressions (`$manufacturer_filter`), as above.

- **Give every optional filter a neutral, match-all default** - a `filter<>` given defaulting to `f''` - so an unsupplied value returns unfiltered rows, matching how `#(filter)` behaves when a value is omitted. Because the given bakes an always-on `where:` into the source, a non-neutral default (e.g. a date floor) applies to *every* read of the source, not just the ones that opt in - so keep defaults neutral. Defaults must be Malloy literals.
- **Givens don't auto-inject a `where:`.** Unlike `#(filter)`, you write the filter expression that references the given yourself (e.g. `where: dimension ~ $given_name`).
- **Not every filter maps cleanly.** A filter with no neutral match-all literal default - e.g. a scalar date/number range like `> @2020-01-01` - is not a good `given:`; keep those on `#(filter)`. Two more cases keep using `#(filter)`: mandatory scoping filters (`required`) and system-injected row-level filters (`implicit`), both below.

Givens are also the substrate for access control - see "Access Control: Source Gating with `#(authorize)`" below.

## Legacy: Parameterizable Filters with `#(filter)`

`#(filter)` is the older, Publisher-specific mechanism for the same idea. Publisher parses the annotation, exposes filter metadata via the API, renders filter widgets in the notebook UI, and **injects `where:` clauses into queries server-side** when callers supply parameters. Prefer `given:` (above) for new models; keep reading and maintaining `#(filter)` on existing models, and keep using it for the two cases `given:` can't cover yet - `required` (mandatory scoping) and `implicit` (system-injected filters), below.

Filters are a **runtime/modeling construct**, not just documentation. They shape governance, query latency (forcing filters keeps result sets bounded), and correctness (see `required` below). They live on the source, never on the consumer: an ad-hoc report or notebook that imports a source inherits and displays that source's filters automatically; it does not (and cannot) declare new ones. If an existing `#(filter)`-based source needs another knob, add it to the source itself, not to the consumer.

### Syntax

```malloy
#(filter) [name=NAME] dimension=DIMENSION type=TYPE [implicit] [required]
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| `name` | No | Unique identifier for the filter; defaults to the dimension name. Used as the API parameter key. |
| `dimension` | Yes | The source dimension this filter targets. Quote with `"..."` if the name contains spaces. |
| `type` | Yes | Comparator (see below). |
| `implicit` | No | Hides the filter from the UI and API summaries. Used for infrastructure concerns the system injects rather than the user. |
| `required` | No | Server returns 400 if a required filter has no value at query time. Use this for governance, latency, and correctness, see below. |

### Filter types

| Type | Malloy clause | Use case |
|------|---------------|----------|
| `equal` | `dimension = 'value'` | Exact match on a single value |
| `in` | `dimension ? 'a' \| 'b' \| 'c'` | Match any of multiple values |
| `like` | `dimension ~ '%value%'` | Substring / pattern matching |
| `greater_than` | `dimension > value` | Range floor (after, minimum) |
| `less_than` | `dimension < value` | Range ceiling (before, maximum) |

### Example

```malloy
#(filter) name=Manufacturer dimension=Manufacturer type=in
#(filter) name=Subject dimension=Subject type=like
#(filter) name=Major_Recall dimension="Major Recall" type=equal
#(filter) name=Recall_After dimension="Report Received Date" type=greater_than
#(filter) name=Recall_Before dimension="Report Received Date" type=less_than
source: recalls is duckdb.table('data/auto_recalls.csv') extend {
  measure:
    recall_count is count()
}
```

For date-range filters, declare two filters with distinct `name` values targeting the same dimension (one `greater_than`, one `less_than`).

### When to use `required`

`required` filters are a correctness, latency, and governance mechanism, not just UX. Mark a filter `required` when:

1. **Modeling correctness, the source's `primary_key:` is only unique under a filter.** If a high-cardinality key is not unique across the whole table but is unique within a scoping dimension, then that scoping dimension MUST be supplied for symmetric aggregation to produce correct numbers. For example, if `events.id` repeats across days but is unique within a single `event_date`, queries that don't pin the date can fan out and return hash-collision-sized garbage (~10²¹). Declare `#(filter) name=Event_Date dimension=event_date type=equal required` so the server refuses queries that don't provide it.
2. **Query latency, the source spans more data than any single query should scan.** A multi-year, multi-region table where every reasonable analysis is scoped to a date range or region: making the date filter required prevents accidental full-table scans.
3. **Partial views** that are only meaningful inside a date range, region, or business segment.
4. **Governance**, an analyst should never query the raw source without a scoping filter applied.

For (1), pair the required filter with a comment explaining the cardinality dependency, and consider also declaring `#(doc)` on the source noting the constraint.

### When to use `implicit`

Use `implicit` for filters the *system* must inject but users should not see. The filter applies; it just doesn't appear in the UI or API filter list.

### Type-aware literals

Publisher formats values based on the dimension's data type, `string` → `'value'`, `boolean` → bare `true`/`false`, `date` → `@YYYY-MM-DD`. You don't quote values yourself in the API call; Publisher handles formatting.

### Bypass

Pass `bypass_filters=true` (REST) or `bypassFilters: true` (POST body) to skip filter injection entirely. Use sparingly, required-filter governance only works if bypass is restricted to trusted callers.

## Access Control: Source Gating with `#(authorize)`

Gate query access to a source with `#(authorize)` over declared `given:` values (`given:` is Malloy's native runtime-parameter mechanism, the going-forward replacement for `#(filter)`). A gate is an `#(authorize)` annotation on its own line directly above the `source:` line, carrying an **unquoted, ordinary Malloy boolean expression**; Publisher grafts that expression onto the source as a row filter before running the query, so a caller it admits nowhere gets **200 with zero rows**, not a 403. A **403** means only that the gate could not be attached at all. A source with no `#(authorize)` annotation of its own or inherited is unrestricted.

```malloy
##! experimental.givens

given:
  ROLE :: string

#(authorize) $ROLE = 'analyst'
source: orders is duckdb.table('orders.parquet') extend {
  measure: order_count is count()
}
```

- **Any legal Malloy boolean expression is a legal gate**, over givens, row fields (including through a join), literals, functions and operators: `org_id in $GROUPS`, `upper(region) = $REGION`, `` `cost center` in $GROUPS ``, `(org_id in $GROUPS or region = $REGION) and amount > $AMOUNTMIN`. There is no allowlist of accepted comparison shapes.
- **A source may declare at most one `#(authorize)` annotation.** Declaring a second on the same source fails the load naming both. Spell OR inside the expression rather than stacking annotations. For a condition too long to read on one line, point the gate at an ordinary boolean dimension instead: `#(authorize) authorized` above the source, over `dimension: authorized is org_id in $GROUPS` inside it; validation follows the reference through.
- **`#(authorize)` only gates from the `source:` line.** The same annotation on a `dimension:`/`measure:`/`join_*:`/`view:` line, or on a top-level `query:`, is refused at load naming the position rather than silently protecting nothing.
- **Every given the gate references must be declared on the entry model's own surface, and must carry no default.** A given the model cannot resolve is refused at load. So is a referenced given declared *with* a default: a caller who supplies nothing would get that default and be admitted or excluded by a value the gate's own line never shows, so it is refused rather than reasoned about case by case. This follows a bare reference through, so a given reached one hop away via `#(authorize) authorized` is checked too.
- **Two shapes load with a warning rather than a refusal.** A gate that references **no given** at all is a fixed predicate, not an access rule keyed on the caller. A gate that **negates a membership test** (`not (org_id in $GROUPS)`) matches every row for an *empty* given instead of none. Both warn and still load, so read the load warnings.
- **Entry point only: not joined, but inherited through `extend`.** The gate applies to the source a query enters through. A gate on a source reached only via `join_*` **never fires**, at any depth, so anything ungated that joins a locked base hands the base's rows to every caller. A source that `extend`s a locked base and declares no gate of its own **does** carry the base's gate; declaring its own annotation replaces it. A source derived from a locked base via a query (`source: z is locked -> { … }`) instead **always carries the base's gate in addition to its own**: the derivation recurses into the base unconditionally, so an own gate does not replace it, and the two combine as separate AND'd entries. Pair a locked base (`#(authorize) false`) with curated extension sources, using access modifiers (`include { public: …, private: * }`), so an extension re-exposes only a curated column surface, and keep sensitive sources out of ungated joins.
- **A derivation that drops a column the gate reads fails CLOSED.** `extend { except: org_id }`, or an `accept:` that omits it, leaves the grafted filter unable to compile, so the request is denied rather than served ungated. The one hole to know: dropping the gated column and then `rename:`-ing a *different* column onto that exact name grafts successfully and binds the gate to the wrong column. Narrow, but real, so don't recycle a gated column's name.
- Comparing a row field to an array-typed given with `=`/`!=` (`org_id = $GROUPS`) compiles and loads cleanly, then fails at query execution with a warehouse conversion error. Use `in` for an array-typed given, not `=`.
- **The quoted-string and file-level forms are refused at load and no longer exist.** `#(authorize) "<expr>"` on the `source:` line, in either quote (`'...'` is refused the same way), a file-level `##(authorize) "<expr>"` applying to every source in the file, and the earlier `internal dimension: authorized is <expr>` form are all retired; the load names the rewrite. Every `.malloy` file in a package compiles at load and any failure aborts the package, so a retired-form gate anywhere in the package is refused. Only a declaring file *outside* the package escapes that: it loads and denies every request instead, with no compile-time hint. See your deployment's reference documentation.
- **A gated source can be persisted, but the gating column freezes.** `storage=` and `#@ preaggregate` refuse a gated source outright; a colocated `#@ persist` is admitted when the gate is provably the entry point's own row filter. The gate still runs live on every query, so rows come back filtered - but the column it filters ON is frozen at build time, so a row whose access decision changes keeps being served under the old decision until the next rebuild. Pair `#@ persist` on a gated source with a freshness window (`fallback="live"`), which is the only control that bounds that - and read `skill:malloy-materialization` for where that window binds, because on a standalone Publisher it does not.

> **Trust caveat.** Givens are **caller-asserted**, anyone who can reach the query API can claim a favorable given, e.g. `{"ROLE":"admin"}`. `#(authorize)` is only a real boundary when it sits behind a trusted tier that sets givens from its own verified context, never directly from an untrusted caller. It is not, on its own, end-user authentication.
>
> **Forward direction.** Givens are how access control is built here, and the planned next step is **identity-bound ("secure") givens** - reserved values a trusted tier populates from a verified token or proxy header, which the caller cannot override - turning `#(authorize)` into a standalone boundary. Model access on `given:` + `#(authorize)` now; it is the surface that carries forward.

Full syntax, inheritance rules, validation, and the error contract are covered in your deployment's `#(authorize)` reference documentation.

## Join Syntax

- Simple join: `join_one: users with user_id`
- Expression join: `join_one: origin is airports on origin_code = origin.code`
- Composite key: `join_one: items on order_id = items.order_id and product_id = items.product_id`
- Multiple joins to same table: `join_one: origin_airport is airports with origin`

**Join Types:** `join_one:` (many-to-one, efficient) | `join_many:` (one-to-many, always safe) | `join_cross:` (many-to-many)

**Verify cardinality** before writing joins: `run: target -> { group_by: fk_col, aggregate: n is count(), having: n > 1, limit: 5 }`. 0 results → `join_one`. Any results → `join_many`.

## After Writing: Check & Review

Check diagnostics after writing. Errors cascade, fix the FIRST error only, then re-check. If errors persist, use the debugging strategy: look at first error, search docs if unsure, fix, repeat.

**Validate with `execute_query`:** Run queries, check distributions, verify measures, confirm joins (no fan-out).

To inspect the sources and fields a model already defines, ground yourself with `get_context`. It returns the package's sources, views, and fields, so there is no separate schema-search step. When you're unsure of Malloy syntax, call `search_malloy_docs` rather than guessing.

## Advanced Patterns

Load the relevant reference file when you encounter these scenarios:

| Scenario | Read |
|----------|------|
| Need pre-aggregated or windowed source | `reference/query-sources.md` |
| Curating access modifiers | `reference/access-modifiers.md` |
| Normalized/ER-style schema (4+ tables, no clear fact table) | `reference/normalized-schemas.md` |
| Formalizing analysis into a model | `reference/analysis-to-model.md` |
| Many-to-many / bridge tables / composite keys | `reference/bridge-tables.md` |

## Done

Step complete. Output: base source files (`.malloy`, one per table) and joined source files (`.malloy`, one per analytical domain).

**Suggest next steps to the user:**

- Open the model in the browser to see it live: `http://localhost:4000/<environmentName>/<packageName>` for the package, or `http://localhost:4000/<environmentName>/<packageName>/<modelPath>` for a single model file. First confirm the running server actually serves this package (it is in the loaded `publisher.config.json`, or mounted live with `--server_root . --watch-env <env>`); a package the server has not loaded returns a 404, so do not hand over a link to a package that was just authored but never loaded.
- Build a notebook with interactive filters over the model (see `skill:malloy-notebooks`).
- Run analysis questions against the model (see `skill:malloy-analysis`).
- When you're ready to serve the model, publishing is out of scope for open-source Publisher v1: self-hosters commit the package to git and use their host's publish path.
