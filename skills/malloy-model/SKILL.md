---
name: malloy-model
description: Build Malloy semantic models with base source and joined source files. Use when creating or modifying .malloy files, user asks to "create a malloy model", "add dimensions", "add measures", "create a source", or any Malloy model authoring task.
---

# Building Malloy Models

## Getting Started (New Projects)

If no `.malloy` files exist yet, do discovery and propose a structure first, then return here to build base source and joined source files. Keep proposals and the analysis behind them in the conversation; Publisher has no workspace document store to write them to.

**File structure convention** (flat layout, Publisher doesn't support cross-directory imports yet):
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
| Derived table (PDT/NDT) | `skill:lookml-review` build-derived-tables guidance |
| UNNEST joins or struct access | `skill:lookml-review` build-unnest guidance |
| Review pass for coverage | `skill:lookml-review` review-coverage guidance |
| Curate pass with visibility seeds | `skill:lookml-review` curate-visibility guidance |

## Base Source Templates

### Base Source (Simple Mode)

```malloy
source: customers is my_conn.table('sales.customers')
extend {
  primary_key: customer_id

  dimension:
    // Use dimension (not rename:) for cleaner column names
    order_type is `Type`
    full_name is first_name || ' ' || last_name
    segment is lifetime_value ?
      pick 'enterprise' when >= 100000
      pick 'mid-market' when >= 10000
      else 'SMB'

  measure:
    customer_count is count()
}
```

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
    days_since_last_order is days(now - last_order_date)
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
| **Views** | None (views belong in analysis) | None |
| **One per** | Physical table or computed source | Analytical domain |

## Key Rules

- **Define joined tables before referencing them**, use `import` statements in multi-file architecture
- **Use `nullif(denominator, 0)` for all division**
- **Alias joined fields before using in `order_by`**: `group_by: yr is table.year`
- **Verify join paths** exist before referencing `a.b.field` (each hop needs explicit join)
- **Pick syntax**: value BEFORE condition, `pick 'Small' when size < 10`
- **`where:` vs `having:`**: Use `where:` for row filters, `having:` for aggregate filters
- **Never use `rename:`**, it's incompatible with `include {}`. Always use `internal:` + `dimension:` for cleaner column names (e.g., mark `` `Type` `` as `internal`, add `dimension: order_type is \`Type\``)
- **Mark raw columns `internal` when a derived dimension replaces them**
- **Check for duplicate rows** before building measures
- When both a combined table (all types) and filtered/split tables exist, prefer the split tables
- **DRY: define measures/dimensions in base source files, not inline in views**

## Parameterizable Filters with `#(filter)`

`#(filter)` annotations declare filterable dimensions on a source. Publisher parses them, exposes filter metadata via the API, renders filter widgets in the notebook UI, and **injects `where:` clauses into queries server-side** when callers supply parameters. This gives you a clean way to expose tunable knobs (date range, region, manufacturer) without hand-rolling parameterization in every query.

Filters are a **runtime/modeling construct**, not just documentation. They shape governance, query latency (forcing filters keeps result sets bounded), and correctness (see `required` below). They live on the source, never on the consumer: an ad-hoc report or notebook that imports a source inherits and displays that source's filters automatically; it does not (and cannot) declare new ones. If an analysis needs a knob the source doesn't expose, the right move is to add a `#(filter)` to the source itself, not to wedge filtering into the consumer.

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

## Join Syntax

- Simple join: `join_one: users with user_id`
- Expression join: `join_one: origin is airports on origin_code = origin.code`
- Composite key: `join_one: items on order_id = items.order_id and product_id = items.product_id`
- Multiple joins to same table: `join_one: origin_airport is airports with origin`

**Join Types:** `join_one:` (many-to-one, efficient) | `join_many:` (one-to-many, always safe) | `join_cross:` (many-to-many)

**Verify cardinality** before writing joins: `run: target -> { group_by: fk_col, aggregate: n is count(), having: n > 1, limit: 5 }`. 0 results → `join_one`. Any results → `join_many`.

## After Writing: Check & Review

Check diagnostics after writing. Errors cascade, fix the FIRST error only, then re-check. If errors persist, use the debugging strategy: look at first error, search docs if unsure, fix, repeat.

**Validate with `malloy_executeQuery`:** Run queries, check distributions, verify measures, confirm joins (no fan-out).

To inspect the sources and fields a model already defines, ground yourself with `malloy_getContext`. It returns the package's sources, views, and fields, so there is no separate schema-search step. When you're unsure of Malloy syntax, call `malloy_searchDocs` rather than guessing.

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

- Build a notebook with interactive filters over the model (see `skill:malloy-notebooks`).
- Run analysis questions against the model (see `skill:malloy-analysis`).
- When you're ready to serve the model, publishing is out of scope for open-source Publisher v1: self-hosters commit the package to git and use their host's publish path.
