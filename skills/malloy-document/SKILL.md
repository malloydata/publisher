---
name: malloy-document
description: Add documentation with #(doc) tags to Malloy models so fields and sources are described in plain language. Use when user asks to "add documentation", "add doc tags", "document the model", or wants fields and sources described for natural-language search and discovery. For declaring parameterizable filters with #(filter), see the malloy-model skill. Filters are a runtime/modeling construct (governance, latency, correctness), not a documentation tag.
---

# Documenting a Malloy Model

Add `#(doc)` tags to describe sources and fields in plain language so they are easy to find and understand:

| Tag | Purpose | Goes on |
|-----|---------|---------|
| `#(doc)` | Plain-language description for natural-language search | source, dimension, measure, view, join |
| `#(filter)` | Declare a parameterizable filter (runtime/modeling concern, see `malloy-model`) | source |

`#(doc)` is a standard Malloy annotation. It documents a field or source with a human-readable description that downstream tools can surface and search against.

## #(doc) Tag

Add before any source, dimension, measure, view, or join. When multiple fields share a keyword, use it once as a block header. Tags and field names are indented under the keyword; tags go on the line(s) directly above the field they annotate.

**Tag ordering** (when a field has multiple tags): `#(doc)` → render tags (`# currency`, `# label`, etc.) → field name. Separate each field group with a blank line:

```malloy
#(doc) Customer who placed the order
join_one: users with user_id

dimension:
  #(doc) Date the order was placed (UTC)
  order_date is created_at::date

measure:
  #(doc) Total revenue from all orders in USD
  # currency
  revenue is sum(total)
```

### Writing Doc Strings for Retrieval

Doc strings power natural-language search: users type plain-English questions and the system matches against your `#(doc)` strings. Write descriptions that match how analysts would search:

- **Include business meaning**, not code mechanics: what it represents, not how it's implemented
- **Include units** (USD, count, percentage) and valid values for categorical fields
- **Avoid Malloy jargon**: never use "filterable", "groupable", "dimension", "measure", "aggregation"

**Good examples:**
- `#(doc) Total revenue from completed orders in USD` matches "what was our revenue?"
- `#(doc) Customer signup date (UTC)` matches "when did the customer join?"
- `#(doc) Order status: pending, processing, shipped, delivered, cancelled` matches "what are the order statuses?"

**Bad examples:**
- `#(doc) Filterable dimension for order status`: no analyst searches for "filterable"
- `#(doc) Groupable by region`: "groupable" is a system concept
- `#(doc) Aggregation of total sales`: "aggregation" doesn't match natural queries

## #(filter): see `malloy-model`

`#(filter)` is also a `#(...)`-shaped annotation, but unlike `#(doc)` it's a **runtime/modeling construct**: it shapes governance, query latency, and correctness, not discoverability. The full reference (syntax, filter types, `required` / `implicit` flags, and when each applies) lives in `malloy-model` § Parameterizable Filters with `#(filter)` alongside the other source-authoring constructs.

One rule worth knowing here: filters live on the source, never on the consumer. Ad-hoc reports and notebooks that import a source inherit its filters automatically; they do not (and cannot) declare new ones.

## `internal:` and `private:`: column-level access in a source

`#(doc)` describes what's exposed. Two access modifiers control what's exposed in the first place, and both live **inside** a source's `include {}` block. They are about the source's public API and data sensitivity, not about documentation, so reach for them when curating which columns callers can pick.

| Mechanism | Layer | Why you reach for it |
|---|---|---|
| `internal:` | Inside a source (one column in `include {}`) | The column **isn't part of your model's public API**. Common reasons: data is messy (empty/garbage, raw JSON, duplicates), or a documented derived dimension already supersedes it, or the raw column exists only to be joined on / referenced internally and shouldn't appear as a dimension callers can pick. The data may be perfectly fine, it's just not what you want exposed. |
| `private:` | Inside a source (one column in `include {}`) | The **data is sensitive**: SSN, raw credit card, password. Governance / security concern; a harder block than `internal:`. |

In one sentence: **`internal:` and `private:` shape what's inside a source's public API; `#(doc)` describes the fields you do expose.**

### Example

A base source pulled from a messy raw table often uses `internal:` to drop raw fields from the public API, while documenting the curated columns with `#(doc)`.

```malloy
// orders_base.malloy
#(doc) Raw orders. Use orders.malloy as the entry point for analysis.
source: orders_base is conn.table('orders_raw')
  include {
    public: id, customer_id, order_date, total
    internal: raw_json_payload, deprecated_status_code, _temp_dedup_marker
  }
  extend {
    primary_key: id
  }
```

```malloy
// orders.malloy
import "orders_base.malloy"

#(doc) Order analysis. Use for revenue, fulfillment, and customer-order joins.
source: orders is orders_base extend {
  // joins, measures, curated dimensions
}
```

The base source stays fully queryable (`run: orders_base -> { ... }` still works); `internal:` only governs which columns appear as public dimensions callers can pick.

## Annotating Columns in Include (Experimental)

With `##! experimental.access_modifiers`, you can add `#(doc)` tags to raw table columns inside `include` blocks. This documents columns without redefining them as dimensions.

```malloy
##! experimental.access_modifiers

source: orders is conn.table('orders') include {
  public:
    #(doc) Order line item identifier
    id

    #(doc) Customer email address
    email

    #(doc) Order status: pending, shipped, delivered
    status

  // internal: only for verified noise (empty cols, raw JSON blobs, duplicates)
}
extend {
  // ... dimensions and measures
}
```

**When to use:**
- Documenting raw columns without creating explicit dimensions
- Curating which columns are public vs internal

## Source-Level Documentation

Document **when to use** a source, not what it contains. Dimensions and measures can already be searched directly, so the source-level `#(doc)` should describe what questions/analyses this source answers.

**Base source files:** Document what the table represents.
```malloy
#(doc) Customer records with demographics and segmentation. One row per customer.
source: customers is conn.table('sales.customers') extend { ... }
```

**Source files:** Document what analytical questions the source answers.
```malloy
#(doc) Customer health analysis. Use for retention, segmentation, churn risk, and lifetime value. For order-level analysis, use order_analysis instead.
source: customer_health is customers extend { ... }
```

**Best practices:**
- Add `#(doc)` to all base source and joined source definitions
- Base source docs: describe what the table is (one row per what)
- Source docs: describe what questions/analyses the source answers
- Documentation happens per-source-file, not in one monolithic file

## Flag Ambiguous Descriptions

After writing `#(doc)` tags, present any that required judgment to the user for confirmation:

| Field | Proposed doc | Confidence | Uncertainty |
|-------|-------------|------------|-------------|
| `total` | "Total order amount in USD" | Medium | Could be gross or net, verified with sample query |
| `status` | "Order status: pending, shipped, delivered" | High | Values confirmed via a query of distinct values |

Only flag fields where the description required assumptions about business meaning, units, or valid values. When in doubt about valid values, run a quick query against the data to confirm them before writing the description. Use `malloy_modelGetText` (or `malloy_packageGet` to see what is in the package) to read the model and `malloy_executeQuery` to check distinct values, for example `run: source -> { group_by: status }`.

## Done

Step complete. Output: `#(doc)` tags added to all public fields and sources.
