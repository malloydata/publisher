---
id: security-destination-not-nameable
tags: security
package: dnn
---

# Security: a published model cannot name a materialization destination

The destination a `#@ persist … storage=` writes to lives in
`materializationDestinations`, a list nothing a user authors resolves against. A
package's models resolve connections through the environment's `connections`, and
those two lists are disjoint — so a model file naming the destination has no such
name in scope, and the package carrying it does not load at all.

This is the surface that mattered most. Ad-hoc query text compiles in restricted
mode, which rejects `connection.table(...)` on its own; a **published model file**
does not, so before the split it resolved on the worker with no control-plane
round-trip at all — the model simply read the destination.

Both halves of the split are proven together here: `dnn` builds into `lake` and
serves from it, while two packages that merely NAME `lake` are refused. One
scenario, one environment, one destination.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.dnn_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 200        |

## Model dnn.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.dnn_orders')

#@ persist name="dnn_daily" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Model reader/reader.malloy

A second package whose model reads the destination's own table directly.

```malloy
source: stolen is lake.table('main.dnn_daily')
```

## Model reader_sql/reader_sql.malloy

A third, reaching for it through embedded SQL — how a reader would exfiltrate a
table whose name they had to guess.

```malloy
source: stolen is lake.sql('SELECT * FROM main.dnn_daily')
```

## Publish

The build resolves `lake` — as a DESTINATION. Same name, different list.

expect binding: daily_orders -> lake

## Query rollup

Served from the materialized table, so the destination IS reachable from the serve
path.

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 100          |
| 2026-01-02 | 200          |

## Rejected reader

The `table()` form dies in the schema fetch, which reports the unresolvable
connection as an import-reference failure. The package does not serve.

cites: import reference failure

## Rejected reader_sql

The `sql()` form says it plainly: the name is not in the config the model compiles
against.

cites: No connection named

## Connection lake (refused)

The connection endpoints cannot reach it either: `sqlQuery` takes a
caller-supplied connection name, and a destination is not in that namespace.

```sql
SELECT 1
```

## Query rollup (again)

Nothing above disturbed the materialized table, and it is still routed.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 100          |
| 2026-01-02 | 200          |
