<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The `#@ persist` annotation

```malloy
#@ persist name="my_dataset.my_table"
source: my_rollup is some_source -> { group_by: ...; aggregate: ... }
```

## What can carry it

**Only `query_source` and `sql_select` sources are persistable** - a source whose definition has a `-> { ... }` pipeline or a `conn.sql("...")`, including one refined by a trailing `extend { ... }`. What is **not** persistable is a *plain* `extend` over a bare `conn.table(...)` (a filtered pass-through). A `#@ persist` on such a source is **refused, not ignored**: the package carries a warning naming the source, and any materialization run that covers it fails with `annotated '#@ persist' but were not recognized as a materializable source`, so a whole-package run builds nothing until you fix it. Persist a query source instead (`source: x is raw -> { where: ...; select: * }`), or remove the annotation.

**A parameterized source must have its argument bound.** `#@ persist` on `high_value(min_amount is 500) -> { ... }` builds one fixed relation; on the free template `high_value -> { ... }` it is refused (`free_parameter`), since there is no single relation to store. A `given` the persisted *query* reads is refused the same way (`given_in_persisted_query`); a given in the source's own `extend` block is applied per caller at read. `reference/storage.md` says where a given may sit.

## The name

- **Quote it, once.** `name="my_table"`, or a path `name="dataset.table"` / `name="project.dataset.table"`. A **bare** `name=my_table` fails the package **load** - so every publish and every restart - with `persist annotation name must be quoted`; it never silently no-ops.
- **Plain identifier segments only**: dot-separated segments of letters, digits, underscores and hyphens. A quoted name holding a space, quote, semicolon or backtick is rejected with its own message, because the name is inlined into DDL.
- **Never quote inside the quotes.** `name="\"Quoted Tbl\""` reaches the build with literal quote characters: the table is created with them in its name, the serve side looks for the unquoted one, and the source silently serves live after a wasted build.
- **One name per table.** Two distinct persist sources that resolve to the same `name=` in the same destination clobber each other (the second build overwrites the first, and dropping one drops the other's). Publisher warns on the package at load and, where `PERSIST_COLLISION_ENFORCE` is set, rejects the publish. A shared *body* is fine (it shares one table by content address); a shared name over different bodies is not.
- **The target's container must exist.** `name="analytics.daily"` puts the table in `analytics`, which the connection must be able to write and which Publisher does not create. The source can *read* anything the connection can read.
- **The name is not the identity.** In a standalone Publisher `name=` is the physical table, rebuilt in place; a hosted deployment may assign the physical name itself. A `storage=` destination takes a single undotted name, since placement there is the destination's.

## Identity and reuse

A persist source's identity is its **`sourceEntityId`**: a content address of its connection, its canonical SQL and its `partition=` layout. Not the name, not the file, not the package version. So:

- **A rebuild with unchanged persist logic reuses the existing table** (skip-if-unchanged); changing the SQL or the partition layout builds fresh under a new address. `--force-refresh` and every scheduler fire defeat the skip; an incremental source is exempt from it and advances by its boundary instead (`reference/refresh.md`).
- **Two sources whose bodies compile to the same SQL share one table**, however they are named, and share one freshness policy and one incremental boundary with it.
- **A republish by itself builds nothing on a standalone server**; reuse is a property of the next *run*.

## Readers of a persisted source

- **An `extend` of a persisted source reads the stored table** and is not a build target of its own: it inherits the parent's tag and the same address, so the two names are one table. To make a distinct non-persisted intermediate, pipe first: `base -> { select: * } extend { ... }`.
- **A query over a persisted source** (`orders is _orders_fact -> { select: * }`, the private-fact / public-wrapper idiom) reads the table too, with the fact's `extend`-block filters applied per caller, and is not a build target.
- **`#@ -persist`** on an extension opts it out: it recomputes from raw on every query and never reads the table. Reach for it when a reader must not see stale rows, and remember it forgoes exactly the work persistence was there to save.
- Which queries over a reader route to the table is decided per query - a join to a non-persisted source or a nested column serves live for the queries that touch it (`reference/storage.md`).

## Every option

| On the tag | What it does | Where it is explained |
|---|---|---|
| `name="..."` | the target table (required) | above |
| `storage=<destination>` | build into a storage destination and serve from it | `reference/storage.md` |
| `partition="a,b"` | lay the stored table out by those columns (`storage=` only) | `reference/storage.md` |
| `refresh="incremental" watermark="col" [merge_key="a,b"]` | refresh by bounded delta instead of a full rebuild | `reference/refresh.md` |
| `freshness.window="24h" freshness.fallback="live"` | per-source freshness objective (hosted deployments) | `reference/refresh.md` |
| `#@ queryMetadata.<key>="v"` (sibling line) | per-source cost-attribution properties | `reference/policy.md` |
| `#@ -persist` (on a reader) | opt a reader out of the table | above |
| `#@ preaggregate grain="..."` (on a measure) | store a derived rollup | `reference/storage.md` |

An unrecognized key on the tag is a warning on the package. The retired per-source `sharing=` and `schedule=` are rejected at publish: scope and schedule are declared once for the package (`reference/policy.md`).
