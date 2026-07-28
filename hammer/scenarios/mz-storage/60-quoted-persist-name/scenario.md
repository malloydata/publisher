---
id: quoted-persist-name
tags: serve-correctness, naming
package: qn
---

# An author-quoted `#@ persist name=` must round-trip write-to-read

`quoteManifestTablePath` passes an already-quoted path through unchanged, on the
documented ground that a `#@ persist name=` value is the author's own canonical SQL.
Its own doc says it must mirror the CREATE side's `quoteTablePath` — which quotes
every segment unconditionally, so for a pre-quoted name the two disagree: the build
creates a table whose name INCLUDES the quote characters while the serve side
resolves the unquoted-inner form. The table is never found, the source silently falls
back to live, and the build was wasted.

Malloy's tag parser honours the escapes, so `name="\"Quoted Tbl\""` reaches the
publisher as `"Quoted Tbl"` with literal quote characters — the pre-quoted case.

Two things pin it: the physical name actually created in the lake, and a stale answer
after the source is mutated (which only holds if the serve side found the table).

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.qn_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |

## Model qn.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.qn_orders')

#@ persist name="\"Quoted Tbl\"" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total is amount.sum()
}
```

## Publish qn

expect binding: daily -> lake

## Connection lake

The created table is named `Quoted Tbl` — the author's quotes delimit the
identifier, they are not part of it. A table named `"Quoted Tbl"` (quotes included)
means the CREATE side re-quoted an already-quoted name.

```sql
SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%Quoted%'
```

Expect:

| table_name |
| ---------- |
| Quoted Tbl |

## Query base

```malloy
run: daily -> { select: order_date, total }
```

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |

## Mutate orders_pg.qn_orders

```sql
INSERT INTO qn_orders VALUES (3, '2026-01-01', 1000);
```

## Query base (again)

Stale ⇒ the serve side resolved the stored table. Fresh (1150) would mean it never
found it and fell back to live.

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |
