---
id: an-older-version-never-serves-a-table-latest-rebuilt
tags: package-versions, serve-correctness
package: shared
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Under scope: package, an older version never serves a shared table latest rebuilt from another definition

Under the default `package` scope, a package's versions share its tables under
one name. An older version may serve a shared table only while that table holds
what its own definition builds. Once latest rebuilds the table from a different
definition, the older version serves live: it must never answer from rows its
own model would not produce. A source both versions define identically keeps
serving the shared table: sharing is the point of the scope. An auto-run of a
version other than latest is refused, because it would rebuild the table latest
serves.

The older version is loaded after a restart, so it binds to the store's run
exactly as a long-running server would, before latest rebuilds.

## Publisher

- PERSIST_STORAGE_MODE: on

## Model anchor/anchor.malloy

```malloy
source: anchor is orders_pg.sql("SELECT 0 as n")
```

## Version shared@1.0.0

```malloy
##! experimental.persistence

source: base is orders_pg.sql("SELECT 1 as n")

#@ persist name="summary" storage=lake
source: summary is base -> { group_by: n }

#@ persist name="stable" storage=lake
source: stable is orders_pg.sql("SELECT 7 as k") -> { group_by: k }
```

## Publish

1.0.0 is latest, so this builds the shared table from its definition.

expect binding: summary -> lake
expect binding: stable -> lake

## Version shared@1.1.0

A different definition, now latest.

```malloy
##! experimental.persistence

source: base is orders_pg.sql("SELECT 2 as n")

#@ persist name="summary" storage=lake
source: summary is base -> { group_by: n }

#@ persist name="stable" storage=lake
source: stable is orders_pg.sql("SELECT 7 as k") -> { group_by: k }
```

## Build refused (version=1.0.0)

cites: is not its latest

## Restart

## Query older (version=1.0.0)

The shared table still holds 1.0.0's build, so 1.0.0 serves from it.

```malloy
run: summary -> { select: n }
```

Expect:

| n |
| - |
| 1 |

servedFrom: storage

## Publish

Latest rebuilds the shared table from its own definition.

expect binding: summary -> lake
expect binding: stable -> lake

## Query latest

```malloy
run: summary -> { select: n }
```

Expect:

| n |
| - |
| 2 |

servedFrom: storage

## Query older (again, version=1.0.0)

The shared table now holds 2. 1.0.0 still answers 1: it stopped reading a table
its own model does not build.

Expect:

| n |
| - |
| 1 |

## Query older stable (version=1.0.0)

Defined identically in both versions, so the table latest's run holds is
exactly what 1.0.0's definition builds, and 1.0.0 still serves it.

```malloy
run: stable -> { select: k }
```

Expect:

| k |
| - |
| 7 |

servedFrom: storage
