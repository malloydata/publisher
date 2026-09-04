---
id: storage-never-falls-back-to-the-warehouse
tags: security, storage, build-control, orchestration
package: nw
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A source that declares `storage=` is never built into its own warehouse

`#@ persist storage=<dest>` is a statement about where this data may be written, and it
is the only one the author made. So when the destination cannot be honored — for any
reason, at any point — the build REFUSES. It never falls back to writing the table into
the source warehouse.

Two reasons, and the second is why "harmless fallback" is the wrong reading. An author
who named a destination has not consented to a copy of their data landing in their own
warehouse. And the credential this server is given for a source warehouse is normally
read-only, so the fallback does not quietly succeed either — it fails on a permission
error whose message names neither the destination nor the annotation, which is a worse
outcome than a refusal that names both.

The rule is independent of WHY no destination is available. A host resolves the
destination, and it may end up with none because the tier is off, because the
organization is not enabled for one, or because it simply omitted the field. Those are
the same fact from the publisher's side, and none of them makes the source warehouse the
right answer.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.nw_orders

| order_id:int | category:text | amount:num |
| ------------ | ------------- | ---------- |
| 1            | books         | 100        |
| 2            | tools         | 200        |

## Model nw.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.nw_orders')

#@ persist storage=lake
source: daily is orders -> {
  group_by: category
  aggregate: total is amount.sum()
}

#@ persist
source: colocated_daily is orders -> {
  group_by: category
  aggregate: n is count()
}
```

## Publish

## Build (orchestrated, pkg=nw)

`daily` is instructed with a physical table and NO destination — the shape a host sends
when it resolved none. Every other guard on this path tests what the instruction asked
for; this one tests what the source DECLARED, which is the only thing that says the
warehouse is off limits.

It fails as its own source and the run carries on, which is what the colocated sibling
below is here to demonstrate. A host that resolved no destination for one source has
partly resolved its list, so failing the run would take down colocated sources that have
no destination to resolve and nothing to do with the refusal.

- daily -> nw_daily_tbl (failed)
- colocated_daily -> nw_colocated_tbl

## Operator orders_pg

`nw_daily_tbl` is absent — the refused source wrote nothing. Everything else is present:
`nw_colocated_tbl` from the instructed sibling, and `colocated_daily` from the auto-run
that publishing kicked off, both of which are colocated and unaffected.

Asked of the warehouse directly rather than inferred from the failure, because the refusal
and the write are separate events: a build could refuse after having already created the
table.

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name
```

Expect:

| table_name       |
| ---------------- |
| colocated_daily  |
| nw_colocated_tbl |
| nw_orders        |

## Note

> The mode-off case is guarded separately and has been: an instruction that DOES carry a
> destination while `PERSIST_STORAGE_MODE=off` is refused on that ground. This scenario
> covers the mirror, where the instruction carries no destination at all and only the
> source's own annotation says the warehouse is not an option.
