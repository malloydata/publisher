---
id: shape-bounds-physical-columns
tags: security, visibility
package: shp
---

# The declared serve shape bounds which physical columns are reachable

A column that exists in the stored table but is absent from the source's declared
`::Shape` must not resolve through the source. This is the second of the two
independent defenses behind a hidden column — the first is the build-time
projection that keeps it out of the table at all (`visibility-leak-guard`) — and
it is the one that decides whether a projection failure is an access-control hole
or an at-rest concern. Worth pinning on its own: nothing else exercises a
physically-present-but-undeclared column, because on every other path the
projection already removed it.

`daily` is built normally, so the captured schema (and therefore the declared
shape) is exactly `order_date, total`. The operator then adds a populated
`secret_col` to the stored table out-of-band, making the physical table strictly
wider than the shape. Three probes then reference it: a direct select, a
`group_by`, and a `select: *` expansion.

Expected: the two direct references fail to resolve, and `select: *` expands over
the declared columns only. `select: *` is the discriminating probe — it must
SUCCEED while returning only declared columns, and its stale total proves the row
came from the snapshot, so the probes really did exercise the storage path against
the widened table rather than falling back to live.

Note on scope: this constructs the state with a column that was never captured,
whereas a build-time projection failure produces one that was captured and then
narrowed away. Both leave the same declared shape reaching Malloy, which is what
bounds resolution, but the equivalence is an argument rather than something
measured here — a source cannot be made to hide a column without a rebuild over
REST (`--init` wipes the store; a plain restart keeps the old model).

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.shp_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model shp.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.shp_orders')

#@ persist name="shp_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total is amount.sum()
}
```

## Publish shp

expect binding: daily -> lake

## Query base

Baseline: the source serves from storage over its two declared columns.

```malloy
run: daily -> { select: order_date, total; order_by: order_date asc }
```

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |
| 2026-01-02 | 200   |

## Operator lake

Smuggle a column into the stored table, out-of-band through the operator's own
read-write DuckLake client. The publisher's captured schema still says
`order_date, total`, so the declared shape does not know this column exists.

```sql
ALTER TABLE shp_daily ADD COLUMN secret_col VARCHAR;
UPDATE shp_daily SET secret_col = 'LEAKED';
```

## Mutate orders_pg.shp_orders

Change the live source so a stale answer is a positive proof of storage routing —
otherwise a refusal could just be the live model refusing an undefined column,
which says nothing about the shape.

```sql
INSERT INTO shp_orders VALUES (4, '2026-01-01', 1000);
```

## Query smuggled column direct (refused)

A direct reference to the undeclared column does not resolve.

```malloy
run: daily -> { select: order_date, secret_col }
```

cites: 'secret_col' is not defined

## Query smuggled column group_by (refused)

Nor does it as a group_by — so the refusal isn't one syntax being rejected.

```malloy
run: daily -> { group_by: secret_col }
```

cites: 'secret_col' is not defined

## Query star expansion

The discriminating probe: it must SUCCEED (so it is really being served, not
refused) while expanding to the DECLARED columns only. `columns: exact` is what
makes this an assertion — a plain Expect table only checks the columns it lists, so
an extra `secret_col` would slip through. The stale 150 proves the row came from
the snapshot, i.e. the probes above ran against the widened physical table rather
than falling back to live.

```malloy
run: daily -> { select: *; order_by: order_date asc }
```

columns: exact

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |
| 2026-01-02 | 200   |
