---
id: chained-build-error-redaction
tags: security, chained, needs-attention
package: cbe
---

# Chained (Tier-3) build errors must not leak the catalog connection secret

Relates to code-review finding #1 (**fixed**). The single-source (Tier-2) build path
runs its failure through `redactConnectionSecrets`; the chained "stack on the parent"
(Tier-3) path used to throw/log the raw `errMessage(err)`, which for a failed DuckLake
catalog RW **attach** could echo the catalog Postgres connection string (incl.
password) into the user-visible build error. Both chained branches now redact.

This scenario induces a chained-build failure by an operator-style out-of-band drop
of the isolated DuckLake catalog database (no role/creds change — safe for other
scenarios and the harness's own Postgres client), then runs a strict orchestrated
chained build of `rollup` (referencing the already-built `daily`) and asserts the
failed build's error carries no catalog password.

**What it pins (GREEN):** with the catalog gone, the strict chained build
fails EARLY at upstream reference resolution (`strict manifest mode forbids fallback
to live`) — a clean, secret-free error, well before the RW attach. So the common
chained-failure error does not leak.

**What it does NOT cover:** the specific sub-path where the reference resolves but
the RW **attach itself** fails is not reached here — dropping the catalog breaks
reference resolution first, so the two can't be isolated black-box, and a creds-only
break would hit the shared test-container role. That branch is covered instead by a
seam-level unit test (`materialization_service.spec.ts`, "redacts connection secrets
in the chained (Tier-3) build refusal") which stubs the chained builder to reject
with a connstring-echoing message and asserts the refusal is redacted. This scenario
stays `needs-attention` only as the reminder of what black-box coverage can't reach.

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

Strict chained rebuild of `rollup` reusing `daily` — fails (reaching the Tier-3
error path).

- rollup -> cbe_rollup__g2 @ cbelake
  reference: daily

## Hook assertChainedErrorRedacted

Assert the failed build's error carries no catalog password.

## Note (since=2026-07-24)

> Finding #1 is FIXED (both chained branches now share one redacted `safeDetail`,
> like Tier-2), but the coverage asymmetry it exposed remains: this scenario can
> only prove the EARLY (strict reference-miss) failure is clean. The
> RW-attach-failure sub-path — the one that can actually echo the connstring — is
> not reachable black-box (dropping the catalog breaks reference resolution first;
> a creds-only break hits the shared container role), so it is pinned by a
> seam-level unit test instead. If the harness ever gains a per-scenario Postgres
> role, revisit and assert the attach failure end-to-end here.
