---
id: cross-connection-destinations
tags: serve-correctness, build-control
package: xc
---

# Cross-connection: two persist sources materialize into two distinct destinations

A package can materialize different sources into different storage destinations;
each serve binding carries its own `storageConnectionName`, so the serve path
routes each source to the right connection independently. This declares a second
DuckLake destination (`lake2`, its own catalog + storage) alongside the default
`lake`, and materializes one source into each — proving cross-connection routing.

## Connection lake2 (type=ducklake)

A second DuckLake destination, separate catalog + storage from `lake`.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.xc_orders

| order_id:int | region:text | amount:num |
| ------------ | ----------- | ---------- |
| 1            | US          | 100        |
| 2            | US          | 50         |
| 3            | EU          | 200        |

## Model xc.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.xc_orders')

#@ persist name="xc_a" storage=lake
source: a is orders -> {
  group_by: region
  aggregate: total is amount.sum()
}

#@ persist name="xc_b" storage=lake2
source: b is orders -> {
  aggregate: grand_total is amount.sum()
}
```

## Publish

`a` lands in `lake`, `b` lands in `lake2` — distinct connections.

expect binding: a -> lake
expect binding: b -> lake2

## Query a

```malloy
run: a -> { select: region, total; order_by: region asc }
```

Expect:

| region | total |
| ------ | ----- |
| EU     | 200   |
| US     | 150   |

## Query b

```malloy
run: b -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.xc_orders

| order_id:int | region:text | amount:num |
| ------------ | ----------- | ---------- |
| 99           | US          | 1000       |

## Query a (again)

Still `150` for US ⇒ served from the `lake` table (routed, stale).

Expect:

| region | total |
| ------ | ----- |
| EU     | 200   |
| US     | 150   |

## Query b (again)

Still `350` ⇒ served from the `lake2` table (routed, stale) — the second
destination routes independently.

Expect:

| grand_total:num |
| --------------- |
| 350             |
