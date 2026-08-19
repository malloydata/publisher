---
name: malloy-dbt-convert
description: Move a dbt pipeline into Malloy - staging and marts become query sources materialized with #@ persist, under the same semantic layer. Use when the user wants Malloy to replace dbt's transformations rather than sit on top of them, or asks what it would take. Read malloy-dbt-adopt first; the semantic layer is the same either way.
---

# dbt: Convert

> Move the pipeline. dbt's staging models and marts become Malloy query sources, materialized
> with `#@ persist`, under the **same** semantic layer you would build on dbt's marts.

Read `skill:malloy-dbt-adopt` first, and build that semantic layer first. It is the same text
in both cases: the sources, measures, and views an analyst or agent queries do not change when
the tables underneath them do. Converting only changes where the tables come from.

## Say the cost out loud before starting

The default is to leave dbt's marts alone. Converting is worth it when a specific pressure
justifies it -- the mart exists only to feed the semantic layer, the dbt model is renames and
casts with no reviewed business logic, or the goal is one fewer scheduled system between raw
data and answer. It is **not** worth it because the SQL looks simple.

What leaves the building with the dbt model, and must be stated to the user:

- **Its tests.** `not_null`, `unique`, `relationships`, `accepted_values`, `dbt_utils.*`,
  `unit_tests`. Malloy has no test framework today. A converted mart is an *untested* mart.
- **Its contracts** and its place in whatever schedule rebuilt it.

Do not decommission a dbt model until the Malloy version has been compared against that
model's own output and the differences are enumerated and accepted. See **Reconcile** below.

## What maps, and how

**Staging models** are renames, unit casts, coalesced booleans, and date truncation. A dbt
staging *view* is a Malloy query source: both recompute on read, so neither needs persisting.
This is the least interesting SQL in the project and it moves over mechanically.

Read what a macro *produced*; never re-implement its templating. `cents_to_dollars` is
`(col / 100)::numeric(16,2)`, so the Malloy is `round(col / 100, 2)`.

**Marts** are `+materialized: table`. In Malloy that is one annotation:

```malloy
#@ persist name="orders"
source: orders_mart is stg_orders extend {
   join_many: order_items_mart on order_id = order_items_mart.order_id
} -> {
   group_by: order_id, customer_id, ordered_at
   aggregate: order_cost is count(order_items_mart.order_item_id)
   calculate: customer_order_number is row_number() {
      partition_by: customer_id
      order_by: ordered_at asc
   }
} extend {
   primary_key: order_id
}
```

No materialization strategy to choose, no incremental config to translate. Read
`skill:malloy-materialization` before writing the annotation; two rules cause most failures:

- **`##! experimental.persistence` on every `.malloy` file in the package**, including files
  that persist nothing. One unflagged file aborts the whole package's build plan.
- **Only a query source is persistable** -- a `-> { ... }` pipeline, optionally refined by a
  trailing `extend { ... }`. A `#@ persist` on a plain `extend` over a table is silently ignored.

A standalone Publisher does not build on publish; trigger a run
(`malloy-pub materialize --environment <env> --package <pkg> --wait`).

**Window functions** are the construct people assume forces them back to SQL. A
`row_number() over (partition by ... order by ...)` closing a dbt mart is a `calculate:` with
`partition_by:` / `order_by:`. A `SUM(...) OVER (... ROWS UNBOUNDED PRECEDING)` is
`sum_cumulative(...)`.

**A stored column that depends on an aggregate** needs a second pipeline stage. dbt's
`orders.sql` computes `count_food_items > 0 as is_food_order` after aggregating; in Malloy,
aggregate in one stage and project the boolean in the next, so it is a real column of the
materialized table rather than an expression evaluated on read:

```malloy
} -> {
   select: *
      is_food_order is count_food_items > 0
}
```

## Split the built table from the semantic source

Name the persisted sources `<name>_mart` and let the semantic layer define `<name>` on top.
This is not cosmetic. dbt's marts are frequently **mutually referential across layers**: a
`customers` mart is built by aggregating `orders`, while the semantic `orders` joins
`customers` for its customer attributes. Build and semantics in one source makes that a cycle
Malloy will reject.

```
staging.malloy      stg_orders, stg_customers, ...          (renames and casts)
marts.malloy        orders_mart, customers_mart, ...        (#@ persist; customers_mart reads orders_mart)
jaffle_shop.malloy  orders, customers, ...                  (keys, joins, measures, views)
```

The semantic file is then byte-identical to the one you would write over dbt's marts, except
for what each source extends. That equality is the point, and it is checkable: run the same
query against both packages and compare.

## The four traps

Every one of these compiles clean and returns a wrong or surprising value.

**1. `joined.count()` counts the outer-join row.** This is the dangerous one. dbt's mart
pattern -- aggregate a child table, `LEFT JOIN` it to the parent -- yields **NULL** for parents
with no children. In Malloy, `order_items.count()` returns **1** for such a parent, because the
outer join produces a row and `count()` counts rows. Use the key:

```malloy
count_order_items is order_items.count()                  // WRONG: +1 per childless parent
count_order_items is count(order_items.order_item_id)     // RIGHT: 0
```

On jaffle-shop, 77 of 9,568 orders have no items, and the wrong form sums to 14,327 against
dbt's 14,250. Nothing errors. Note also that the layers still *describe* those rows
differently afterwards: dbt has NULL, Malloy has 0, so a downstream boolean is NULL in dbt and
`false` in Malloy. Pick one and write it down.

**2. Decimal money becomes floating point.** dbt casts to `numeric(16,2)`; Malloy's `round()`
stays a double. Sums agree to ~1e-9 relative, not bit-for-bit: `105826.17999999996` against
`105826.18`. If exact decimal money is a requirement, that is an argument for leaving the cast
in dbt.

**3. Surrogate keys will not match.** `dbt_utils.generate_surrogate_key([...])` is an md5 with
specific null handling. A `concat()` of the same parts is just as unique and far clearer, but
the column's *values* differ from dbt's, so nothing can join across the two layers on it. Say so.

**4. Join targets need primary keys.** Staging sources are query sources with no key, so close
them with `} extend { primary_key: ... }` -- or use the explicit `join_one: x on a = x.b` form.

## Reconcile against dbt's tables

You have the strongest possible oracle: dbt's built marts are sitting in the warehouse. Compare
row counts and every aggregate, per mart, before trusting the rebuild. The loop and the claim
discipline are in `malloy-dbt-adopt`'s `reference/reconcile.md`; the only difference is the
baseline -- dbt's **tables** here, rather than its metrics.

Then reconcile the semantic layer too, by running the same queries against both the
dbt-marts package and the converted one. Integers should match exactly; money may differ in
the last decimal places for the reason above. Report which, rather than rounding until the
difference disappears.
