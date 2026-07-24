---
id: extended-source-inherits-persist
tags: eligibility, safety, known-red
package: esi
---

# Extending a persisted source must not inherit its `#@ persist` (malloy #3012)

**KNOWN-RED — documents malloy-core bug malloydata/malloy#3012 (fix not yet adopted).**

A source that EXTENDS a persisted source inherits its `#@ persist` annotation —
including `name=` — so `getBuildPlan()` treats the extended READER as a second
build target writing the SAME table. Per the docs, extending a persisted source
should just read the pre-built table, not re-materialize it. Malloy compiles it
fine, so the model publishes; the duplicate same-named target only bites
downstream — the orchestrated/host build path issues two builds to one physical
table.

The publisher's own collision guard (`persistenceCollisionWarnings`) does NOT catch
this: the base `daily` and the extended `daily_ext` are content-addressed
identically (same `sourceEntityId`), and the guard dedups build-plan sources by
`sourceEntityId` (to collapse identical-content imports), so the two collapse to
one and no warning fires. Auto-run dedups the same way (so it happens not to
double-build locally), but the build plan still exposes the duplicate target to a
host that assigns names per source.

This hook asserts the extended reader is NOT a separate build target — RED now (it
inherits the tag), GREEN once malloy #3012 (own-annotation `persistDeclared`) is
adopted and `daily_ext` reads `daily`'s table instead of re-materializing.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.esi_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model esi.malloy

`daily_ext` extends the persisted `daily` intending only to READ it (add a derived
field), but inherits `#@ persist name="esi_daily"`.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.esi_orders')

#@ persist name="esi_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

source: daily_ext is daily extend {
  dimension: doubled is total_amount * 2
}
```

## Hook assertNoDuplicateInheritedTarget

The build plan must have exactly ONE build target for `esi_daily` (the base
`daily`). RED today: `daily_ext` inherits the tag and appears as a second target.

## Note (since=2026-07-24)

> Two things here. (1) UPSTREAM: malloy propagates `#@ persist` through `extend`,
> so an extended reader becomes a duplicate build target (fix:
> malloydata/malloy#3012 — a `persistDeclared` flag from the source's own
> annotation only). This is the primary fix; this scenario goes green when we
> adopt a malloy carrying it. (2) OURS: `persistenceCollisionWarnings` misses this
> collision because it dedups by `sourceEntityId` and the base + extended reader
> share one. Consider hardening the guard to flag two DISTINCT source names
> resolving to the same persist target even when their content address matches, as
> defense-in-depth until/unless the upstream fix is guaranteed present.
