# Absorb dbt's Transforms (Later, Optional)

> Moving staging and mart SQL out of dbt and into Malloy with `#@ persist`. This is a separate,
> later decision from building the semantic layer - and usually the wrong first move.

## Read this before starting

The default is to model **on top of** dbt's marts. The marts are reviewed and tested; rebuilding
them in Malloy discards the tests that guard them and Malloy has nothing to replace them with.
Absorbing transforms is worth doing when a specific pressure justifies it:

- the mart exists only to feed the semantic layer, and nothing else reads it
- the dbt model is a rename-and-cast layer with no business logic worth reviewing
- you want one fewer scheduled system between the raw data and the answer

It is **not** worth doing because the SQL looks simple. A dbt model that is one `LEFT JOIN` away
from correct is still a tested contract.

## What maps cleanly

**Staging models.** Renames, unit casts, coalesced booleans, date truncation. This is the least
interesting SQL in a dbt project and it moves over mechanically. A dbt staging *view* is a Malloy
query source: both recompute on read.

A macro is not a thing to reproduce - read what it produced. `cents_to_dollars` is
`(col / 100)::numeric(16,2)`, so the Malloy is `round(col / 100, 2)` written where it applies.
Malloy has no macro layer, and inventing one is not the goal.

**Marts.** A dbt mart is a `+materialized: table`; in Malloy it is one annotation on a query
source:

```malloy
#@ persist name="orders"
source: orders is stg_orders extend {
   join_many: order_items on order_id = order_items.order_id
} -> {
   group_by: order_id, customer_id, ordered_at
   aggregate: order_cost is order_items.supply_cost.sum()
   calculate: customer_order_number is row_number() {
      partition_by: customer_id
      order_by: ordered_at asc
   }
} extend {
   primary_key: order_id
}
```

There is no materialization strategy to choose and no incremental config to translate. Read
`skill:malloy-materialization` before writing the annotation; the two rules that matter most:

- **`##! experimental.persistence` on every `.malloy` file in the package**, including files that
  persist nothing. One unflagged file aborts the whole package's build plan.
- **Only a query source is persistable.** A `#@ persist` on a plain `extend` over a table is
  silently ignored.

**Window functions.** A `row_number() over (partition by ... order by ...)` closing a dbt mart is
a `calculate:` with `partition_by:` / `order_by:`. This is the construct people assume forces them
back to SQL, and it does not.

## The four traps

**1. `joined.count()` counts the outer-join row.** This is the one that will silently corrupt a
count, and it is worth checking every time. dbt's mart pattern - aggregate a child table, then
`LEFT JOIN` it to the parent - yields **NULL** for parents with no children. In Malloy,
`order_items.count()` returns **1** for such a parent, because the outer join produces a row and
`count()` counts rows.

Use `count(<joined>.<key>)`, which counts distinct non-null keys and returns 0:

```malloy
// WRONG: adds 1 for every parent with no children
count_order_items is order_items.count()
// RIGHT
count_order_items is count(order_items.order_item_id)
```

On dbt's own jaffle-shop, 77 of 9,568 orders have no items, and the wrong form sums to 14,327
against dbt's 14,250. Nothing errors; the number is just wrong. Note also that the two layers
still describe those rows differently afterwards - dbt has NULL, Malloy has 0, so a downstream
boolean is NULL in dbt and `false` in Malloy. Decide which you want and write it down.

**2. Decimal money becomes floating point.** dbt casts to `numeric(16,2)`; Malloy's `round()`
stays a double. Sums agree to ~1e-9 relative, not bit-for-bit: `105826.1800000045` against
`105826.18`. If exact decimal money is a requirement, that is an argument for leaving the cast in
dbt.

**3. Surrogate keys will not match.** `dbt_utils.generate_surrogate_key([...])` is an md5 with
specific null-handling. Reproducing it exactly is rarely worth it; a `concat()` of the same parts
is just as unique. But the column's *values* then differ from dbt's, so nothing may join across
the two layers on that key. State it.

**4. Join targets need primary keys.** `join_one: x with key` requires a `primary_key` on the
target. Staging sources are query sources with no key, so close them with
`} extend { primary_key: ... }` - or use the explicit `join_one: x on a = x.b` form.

## Prove it before you delete anything

You have the strongest possible oracle here: dbt's built marts are sitting in the warehouse. Do
not decommission a dbt model until the Malloy version has been compared against its output, row
counts and aggregates both, and the differences are enumerated and accepted rather than
discovered. See `reference/reconcile.md`; the loop is the same, with dbt's tables as the baseline
instead of its metrics.

Then be explicit with the user about what left the building with the dbt model: its tests, its
contracts, and its place in whatever schedule rebuilt it.
