# Build the Binding Layer (Step 5)

> Emit one Malloy source per dbt model that names the physical relation and carries dbt's
> column documentation. Keep it separate from the semantic layer.

## Why a separate layer

Two files, two jobs:

- **`bindings.malloy`** - generated. Names the physical relation, lists every column, carries dbt's descriptions. When the warehouse schema changes, regenerate this file and the change is a reviewable diff.
- **the semantic layer** - the keys, joins, measures, and views. Names no dataset, so a dev-to-prod relocation or a renamed table never touches it.

Generate the bindings; never hand-edit them. If a binding needs a change, the fix belongs in dbt or in the generator.

## The shape

A binding is a **query source** (`-> { select: ... }`), not a plain `extend` over a table. That matters for two reasons: `select:` accepts `#(doc)` on a passthrough column, which an `extend` block does not, and a query source is the only shape `#@ persist` can materialize and serve from later (`skill:malloy-materialization`).

```malloy
#(doc) Order overview data mart, one row per order.
source: orders_binding is duckdb.table('data/orders.parquet') -> {
  select:
    #(doc) The unique key of the orders mart.
    order_id

    #(doc) The foreign key relating to the customer who placed the order.
    customer_id

    #(doc) The timestamp the order was placed at.
    ordered_at
}
```

Then the semantic layer extends it and names no table:

```malloy
import "bindings.malloy"

#(doc) Order overview data mart, one row per order.
source: orders is orders_binding extend {
  primary_key: order_id
  join_one: customers with customer_id
  measure: order_total is order_total_raw.sum()
}
```

## Rules

1. **Every column from `catalog.json`, in ordinal order.** The binding is the schema contract; a partial column list turns a schema change into a silent omission instead of a diff. Requires `dbt docs generate`.
2. **`#(doc)` from `manifest.json` for every column that has a description.** Undocumented columns are still listed, just bare. This is the single highest-value part of the conversion: descriptions are authored once in `schema.yml` and reach every Malloy consumer.
3. **A multi-line dbt description becomes consecutive `#(doc)` lines.** Do not collapse it into one line.
4. **Rename a column when a metric needs its name.** A dbt metric named after the column it aggregates cannot keep that name in Malloy (see `_concepts.md` § Naming). Rename the passthrough here, and say why in its doc:

   ```malloy
   #(doc) The total amount of the order in USD including tax.
   #(doc) Renamed from `order_total`: the dbt metric `order_total` owns that name in the semantic layer.
   order_total_raw is order_total
   ```

   Everything downstream - measures, filters, group-bys - then references `order_total_raw`. Be consistent; a half-applied rename compiles and silently aggregates the wrong field only if the two names both exist, which is exactly the case here.
5. **One binding per dbt model you are converting**, named `<model>_binding`. Do not merge two dbt models into one binding.
6. **No business logic.** No filters, no derived dimensions, no measures. If you are tempted, it belongs in the semantic layer, or in dbt.

## Generating and checking

Emit deterministically: sort sources by name, keep columns in catalog order, and **never write a timestamp or a generation date into the file**. A timestamp makes every regeneration a diff and destroys the drift check.

Give the generator a `--check` mode that regenerates in memory and diffs against the committed file, exiting non-zero on drift. That is the gate that keeps the bindings honest in CI: bindings are relation-level, so a renamed *column* only surfaces when Malloy compiles against the real schema.

## Ordering

A join target must be defined before the source that joins it. Sort the sources so dependencies come first (topological order over the foreign entities). With `import`, sources may live across files, but within a file the order is load-bearing.
