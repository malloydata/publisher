---
id: failed-run-reclaims-its-tables
tags: lifecycle, build-control
package: prt
---

# A part-way failed build reclaims the tables it created

A run that fails commits NO manifest, and manifest-driven GC only drops names a
manifest records — so a table an earlier source in that run already wrote would be
unreachable forever. For DuckLake that is data plus Parquet files at rest. The build
now reclaims those before rethrowing.

`b` is chained on `a` so the order is deterministic: `a` builds, then `b`'s CTAS fails
because its caller-assigned name points at a schema nobody provisioned.

The second half is the guard that makes the reclaim safe. Auto-run assigns STABLE
names, so a rebuild writes in place over the very table the previous manifest still
serves — dropping that to tidy up would take a working source offline. So a name any
`MANIFEST_FILE_READY` run still references is kept, and only unreferenced names (the
generational ones, where the leak actually bites) are dropped.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.prt_orders

| order_id:int | k:text | amount:num |
| ------------ | ------ | ---------- |
| 1            | x      | 100        |
| 2            | x      | 50         |

## Model prt.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.prt_orders')

#@ persist name="prt_a" storage=lake
source: a is orders -> {
  group_by: k
  aggregate: total is amount.sum()
}

#@ persist name="prt_b" storage=lake
source: b is a -> {
  aggregate: grand is total.sum()
}
```

## Build refused (orchestrated, pkg=prt)

`a` builds into an unreferenced generational name; `b` then fails because
`nosuchschema` does not exist. The run ends FAILED with no manifest.

- a -> prt_a__g1 @ lake
- b -> nosuchschema.prt_b__g1 @ lake

## Connection lake (rows=0)

`prt_a__g1` is gone — reclaimed on the failure path. Without that it would be
unreachable forever: no manifest names it, so no GC can ever find it.

```sql
SELECT table_name FROM information_schema.tables WHERE table_name = 'prt_a__g1'
```

## Build (orchestrated, pkg=prt)

Now a SUCCESSFUL build, so a committed manifest references `prt_a__g2`.

- a -> prt_a__g2 @ lake

## Connection lake (rows=1)

```sql
SELECT table_name FROM information_schema.tables WHERE table_name = 'prt_a__g2'
```

## Build refused (orchestrated, pkg=prt)

A failing run that rebuilds `a` at the SAME name the live manifest serves — the
stable-name case. `b` fails again.

- a -> prt_a__g2 @ lake
- b -> nosuchschema.prt_b__g2 @ lake

## Connection lake (rows=1)

`prt_a__g2` SURVIVES. A live manifest still references it, so reclaim leaves it alone
rather than taking a working source offline.

```sql
SELECT table_name FROM information_schema.tables WHERE table_name = 'prt_a__g2'
```
