---
id: failed-run-reclaims-its-tables
tags: lifecycle, build-control
package: prt
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A part-way failed build leaves no table unreachable

Manifest-driven GC only drops names a manifest records. So the danger in a build
that fails half way is not the failure — it is a table an earlier source already
wrote that no manifest ever names. For DuckLake that is data plus Parquet files at
rest, unreachable forever: no manifest names it, so no sweep can ever find it.

**The rule: every table a run writes ends up named by a manifest, or gone.** There
are two ways to honour it, and the publisher uses each where it fits.

A run that produces SOMETHING commits a manifest and records both halves — the
sources that built, and a `failures` entry naming each source that did not and
why. The tables it wrote are named, so they are reachable, and the sources that
did materialize stay usable rather than being thrown away with the one that
failed. That is this scenario.

A run that produces NOTHING has no manifest to record anything in, so it reclaims
instead — see the Note for why that path is not reachable from here.

`b` is chained on `a` so the order is deterministic: `a` builds, then `b`'s CTAS
fails because its caller-assigned name points at a schema nobody provisioned.

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

## Build (orchestrated, pkg=prt)

`a` builds; `b` then fails because `nosuchschema` does not exist. The run does NOT
fail — it reaches `MANIFEST_FILE_READY` and commits a manifest recording `a`'s
table and `b`'s reason.

- a -> prt_a__g1 @ lake
- b -> nosuchschema.prt_b__g1 @ lake (failed)

## Connection lake_probe (rows=1)

`prt_a__g1` SURVIVES, and that is the point: the committed manifest names it, so
it is reachable to manifest-driven GC like any other recorded table. Dropping it
would also have thrown away a source that built perfectly well.

```sql
SELECT table_name FROM information_schema.tables WHERE table_name = 'prt_a__g1'
```

## Connection lake_probe (rows=0)

Nothing was left behind for `b`. Its entry is a recorded FAILURE carrying the name
it was headed for, not a table that exists — which is why the step above has to
assert against `failures` rather than read that name and call it built.

```sql
SELECT table_name FROM information_schema.tables WHERE table_name = 'prt_b__g1'
```

## Build refused (orchestrated, pkg=prt)

Instructing ONLY the source that cannot build. Every instructed source fails, so
the run produced nothing — and a build with no output must not report itself as a
success with errors attached. It reaches FAILED.

- b -> nosuchschema.prt_b__g2 @ lake

## Connection lake_probe (rows=1)

The earlier run's table is untouched by the failed one. A run reclaims only what
IT created, never a table an earlier successful run wrote and a live manifest may
still serve.

```sql
SELECT table_name FROM information_schema.tables WHERE table_name = 'prt_a__g1'
```

## Note (since=2026-09-01)

> **The failure-path reclaim is no longer reachable from this harness, and may not
> be reachable at all.** Worth a decision, because the guards below were written
> for a path that now almost never runs.
>
> `builtThisRun` and `builtSources` are appended together, so "every source failed"
> — the throw that ends a run producing nothing — always implies there is nothing
> to reclaim. The only remaining trigger is an error that escapes the per-source
> `try` AFTER a source has already built, and the per-source `try` wraps
> `buildOneSource` entirely: a bad destination, a failed CTAS and a missing schema
> are all captured as per-source failures. What is left is
> `assertMaterializationEligible` on a later storage-targeted source — real in
> production, where a host instructs by `sourceID` a source that a model edit has
> since made ineligible, but not expressible here: `## Build (orchestrated)`
> resolves every source through the build plan's `sourceEntityId`, and a refused
> source has no plan entry to resolve (see `host-binding-honors-row-level-access`).
>
> So `reclaimStorageTablesFromFailedRun` is pinned by unit tests only, and that is
> where it stays: the harness being unable to name a path is not evidence production
> cannot take it. The open option is growing the step so it can instruct by
> `sourceID`; the guards are NOT up for removal on the strength of how rarely the
> reclaim now fires, because each one closes a way to destroy live data.
>
> Those guards: reclaim is ORCHESTRATED-ONLY. The
> still-referenced check reads this environment and package only, and a BuildID
> carries no environment input — so two environments sharing a destination can
> resolve a source to the same physical name, and a reclaim trusting a
> per-environment check could drop a table another environment is serving. That
> exact shape caused a cross-environment data-loss incident on the hosted side.
> Generational (host-assigned) names remove the collision instead of racing it, and
> auto-run's stable names are overwritten in place by the next build, so skipping
> them forgoes little. The durable fix is refusing a colliding persist target at
> validation time — today `persistenceCollisionWarnings` only looks WITHIN a
> package, so a cross-package or cross-environment collision is undetected (see
> `cross-environment-same-name`).
