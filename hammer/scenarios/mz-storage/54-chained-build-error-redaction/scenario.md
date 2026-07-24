---
id: chained-build-error-redaction
tags: security, chained, needs-attention
package: cbe
---

# Chained build errors must not leak the catalog connection secret

Every storage build path attaches its destination READ-WRITE, and a failing DuckDB
statement echoes the offending SQL — so an unredacted build error can carry the
catalog Postgres connection string, password included, into the user-visible run
`error`. Each path must run its failure through `redactConnectionSecrets`.

This scenario induces a chained-build failure by an operator-style out-of-band drop
of the isolated DuckLake catalog database (no role/creds change — safe for other
scenarios and the harness's own Postgres client), then runs a strict orchestrated
chained build of `rollup` (referencing the already-built `daily`) and asserts the
failed build's error carries no catalog password.

**What it pins (GREEN):** with the catalog gone, the strict chained build
fails EARLY at upstream reference resolution (`strict manifest mode forbids fallback
to live`) — a clean, secret-free error, well before the RW attach. So the common
chained-failure error does not leak.

**What it does NOT cover:** the sub-path where the reference resolves but the RW
**attach itself** fails — the one that can actually echo a connstring. Dropping the
catalog breaks reference resolution first, and a creds-only break would hit the
shared test-container role, so the two can't be isolated black-box. That branch is
pinned by a seam-level unit test instead (`materialization_service.spec.ts`,
"redacts connection secrets in the chained build refusal").

## Connection cbelake (type=ducklake)

Its own isolated DuckLake catalog + storage, so dropping its catalog DB affects
only this scenario.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cbe_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model cbe.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cbe_orders')

#@ persist name="cbe_daily" storage=cbelake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="cbe_rollup" storage=cbelake
source: rollup is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

Both build into `cbelake` (catalog exists, good creds).

expect binding: daily -> cbelake
expect binding: rollup -> cbelake

## Hook dropCatalog

Operator drops the isolated catalog DB out-of-band; the next build will fail.

## Build refused (orchestrated, strict, pkg=cbe)

Strict chained rebuild of `rollup` reusing `daily` — fails (reaching the chained
error path).

- rollup -> cbe_rollup__g2 @ cbelake
  reference: daily

## Hook assertChainedErrorRedacted

Assert the failed build's error carries no catalog password.

## Note (since=2026-07-24)

> Coverage gap, not a product gap: this scenario can only prove the EARLY (strict
> reference-miss) failure is clean. Reaching the RW-attach failure needs a
> per-scenario Postgres role the harness doesn't have yet — if it gains one,
> revisit and assert the attach failure end-to-end here instead of at the unit seam.
