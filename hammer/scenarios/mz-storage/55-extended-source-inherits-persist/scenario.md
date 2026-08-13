---
id: extended-source-inherits-persist
tags: serve-correctness, safety
package: esi
---

# An extension of a persisted source is a second READER of one table, not a second table

The user flow: author a persisted source `daily`, then `daily_with_avg` that extends it
to add a derived field, expecting `daily_with_avg` to READ `daily`'s materialized table
rather than re-materialize.

Malloy propagating `#@ persist` through `extend` is **by design**, and load-bearing:
without it an extension of a persisted source could never be served from a table at all.
`#@ -persist` is the documented opt-out (see `opt-out-persist-recomputes`).

So the extension appearing in the build plan is correct and necessary — it is how the
publisher knows the extension may read the table. What must never happen is a second
**table**: `daily` and `daily_with_avg` compile to identical materialization SQL (an
`extend` adds query-time fields and changes no SQL), so they share one content address,
one manifest entry, and one physical table.

This scenario pins that shape from three sides: both sources are in the plan, both resolve
to ONE content address (the `entity` column), and both answer correctly. That the
extension routes its READ to the base's table — rather than quietly recomputing live — is
proven separately by `extend-routes-to-the-base-table`, which mutates the warehouse so a
routed read is distinguishable from a live one.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.esi_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model esi.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.esi_orders')

#@ persist name="esi_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate:
    total_amount is amount.sum()
    num_orders is count()
}

source: daily_with_avg is daily extend {
  dimension: avg_order_value is total_amount / num_orders
}
```

## Publish

Materialize the package. One table is built; both sources read it.

## Query daily

`daily` serves from its materialized table.

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query daily_with_avg

`daily_with_avg` serves too — reading `daily`'s table and computing its average at
query time (not a stored column).

```malloy
run: daily_with_avg -> { select: order_date, total_amount, avg_order_value; order_by: order_date asc }
```

Expect:

| order_date | total_amount | avg_order_value |
| ---------- | ------------ | --------------- |
| 2026-01-01 | 150          | 75              |
| 2026-01-02 | 200          | 200             |

## Build targets

Both sources are in the plan — the extension has to be, or nothing could bind its read —
and both carry the inherited `name=`. The `entity` column is the assertion that matters:
one content address (label `A`) across both, so one table.

Expect:

| source         | writes    | entity |
| -------------- | --------- | ------ |
| daily          | esi_daily | A      |
| daily_with_avg | esi_daily | A      |

## Note (since=2026-08-13)

> Rewritten. This scenario previously asserted exactly ONE plan row, i.e. that the
> extension must not appear as a build-plan source at all, and was tagged `known-red`
> pending malloydata/malloy PR 3012 (which would have keyed `persist` on a source's OWN
> annotation).
>
> PR 3012 was **closed unmerged**. Its replacement, malloydata/malloy#3029, deliberately
> leaves `Model.getBuildPlan()` unchanged and documents one-table-many-sources as "the
> normal case, not an edge case"; the grouping lives in the new `Runtime.getBuildTargets`,
> which reports one target carrying every source that maps onto it. So the old expectation
> was not merely waiting on an upstream fix — it asserted the opposite of the design, and
> meeting it would break serving: the publisher finds an extension's read binding by
> looking the address up in `buildPlan.sources`, so an extension absent from the plan
> cannot be routed.
>
> What the old prose got right, and what has since been fixed: the duplicate plan entry
> was a landmine for a host that materializes per SOURCE. That is now handled on both
> sides of the build — the publisher writes each physical table once and refuses (or warns
> on) two definitions colliding on one table, and it binds every source sharing an address
> so the extension routes instead of one alias silently winning by build order.
>
> Residual, deliberately not asserted here: the wire build plan reports per-source rows, so
> a host that mints a physical name per source has to group by `sourceEntityId` first or it
> will ask for two tables for one address. The publisher cannot resolve that on the host's
> behalf — declining one of the two instructions would leave the host holding an anchor for
> a table nothing ever wrote — so it meters the condition instead, as
> `publisher_materialization_shared_address_instructions_total`.
