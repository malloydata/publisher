# Source Shape and Naming

> How to write a source that carries dbt's column documentation, and the two naming
> collisions that are compile errors on essentially every real dbt project.

## Carry dbt's column docs with an annotated projection

The highest-value, lowest-risk part of adopting dbt is the documentation: descriptions are
authored once in `schema.yml` and reach Malloy, the Explorer UI, and any agent reading the
model over MCP. Getting them there has one specific requirement.

**`#(doc)` attaches to a field you declare, not to a passthrough table column.** Three things
that do not work, all verified:

```malloy
// FAILS: a field cannot reference itself
source: orders is duckdb.table('orders.parquet') extend {
  dimension:
    #(doc) The total amount of the order in USD including tax.
    order_total is order_total
}

// FAILS: "no viable alternative at input" -- a bare field cannot be re-annotated in extend
source: orders is duckdb.table('orders.parquet') extend {
  #(doc) The total amount of the order in USD including tax.
  order_total
}
```

**What works is a `select:` projection**, which accepts annotations on passthrough columns.
Project the columns, then extend with the semantics:

```malloy
#(doc) Order overview data mart. One row per order.
source: orders is duckdb.table('data/orders.parquet') -> {
  select:
    #(doc) The unique key of the orders mart.
    order_id

    #(doc) The foreign key relating to the customer who placed the order.
    customer_id

    #(doc) The timestamp the order was placed at.
    ordered_at
} extend {
  primary_key: order_id
  join_one: customers with customer_id
  measure: order_total is order_total_raw.sum()
}
```

This shape has a second benefit: a query source is the only thing `#@ persist` can
materialize, so the same structure is ready for `skill:malloy-materialization` if the mart is
expensive enough to warrant it.

Rules for the projection:

1. **Every column from `catalog.json`, in ordinal order.** The projection is the schema
   contract; a partial list turns a schema change into a silent omission instead of a visible
   one. When the warehouse schema changes, re-read `catalog.json` and update it -- the change
   should read as a diff in one place.
2. **`#(doc)` from `manifest.json` for every column that has a description.** Undocumented
   columns are still projected, just bare.
3. **A multi-line dbt description becomes consecutive `#(doc)` lines.** Do not collapse it.
4. **No business logic in the projection.** No filters, no derived dimensions, no measures.
   Those belong in the `extend` -- or in dbt.

## Collision 1: a metric named after its own column

A dbt metric that aggregates the column it is named after -- metric `order_total` summing
column `order_total` -- fails with **"Cannot redefine 'order_total'"**. Something has to move,
and there is a right answer: **keep the metric name**, because that is the name analysts and
agents search for, and rename the passthrough column in the projection.

```malloy
    #(doc) The total amount of the order in USD including tax.
    #(doc) Renamed from `order_total`: the dbt metric `order_total` owns that name here.
    order_total_raw is order_total
```

Everything downstream -- measures, filters, group-bys -- then references `order_total_raw`.
Apply it consistently: after the rename **both** names can plausibly exist in a reader's head,
and aggregating the wrong one compiles clean and returns a wrong number. On jaffle-shop this
hits six metrics (`order_total`, `tax_paid`, `order_cost`, `count_lifetime_orders`,
`lifetime_spend_pretax`, `lifetime_spend`), so expect it, and record the rule in your notes.

## Collision 2: a measure named after its source

A `measure: customers is count(customer_id)` inside `source: customers` is fine -- source and
field namespaces do not collide. dbt projects do this routinely (a `customers` metric on the
`customers` semantic model). No workaround needed.

## Ordering and joins

A join target must be defined before the source that joins it, so sort sources so that
dependencies come first (a topological order over the foreign entities). `join_one: x with key`
also requires a `primary_key` on the target; if the target has no primary entity in dbt, either
declare the key from evidence or use the explicit `join_one: x on a = x.b` form, and record
that the cardinality is unverified.
