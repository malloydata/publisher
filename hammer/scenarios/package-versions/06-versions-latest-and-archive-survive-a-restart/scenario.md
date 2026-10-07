---
id: versions-latest-and-archive-survive-a-restart
tags: package-versions, durability
package: sales
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Published versions, latest and archive state survive a restart, and publishing goes on from there

The registry is durable. After a restart, the server serves every version it
published, from where latest was last pointed, with archived versions still
archived. It also still knows each version's content: the same content again is
placement, and other content under a published version is still refused.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_PACKAGE_VERSIONING: on

## Model anchor/anchor.malloy

```malloy
source: anchor is orders_pg.sql("SELECT 0 as n")
```

## Version sales@1.0.0

```malloy
source: answer is orders_pg.sql("SELECT 1 as n")
```

## Version sales@1.1.0

```malloy
source: answer is orders_pg.sql("SELECT 2 as n")
```

## Version sales@1.2.0

```malloy
source: answer is orders_pg.sql("SELECT 3 as n")
```

## Latest sales@1.1.0

## Archive sales@1.2.0

## Restart

The store is kept: no re-init, no re-publish.

## Versions sales

Expect:

| version | latest | archived |
| ------- | ------ | -------- |
| 1.2.0   | false  | true     |
| 1.1.0   | true   | false    |
| 1.0.0   | false  | false    |

## Query latest

```malloy
run: answer -> { select: n }
```

Expect:

| n |
| - |
| 2 |

## Query first (version=1.0.0)

```malloy
run: answer -> { select: n }
```

Expect:

| n |
| - |
| 1 |

## Query archived (version=1.2.0, refused)

```malloy
run: answer -> { select: n }
```

cites: is archived

## Version sales@1.0.0

```malloy
source: answer is orders_pg.sql("SELECT 1 as n")
```

## Version sales@1.0.0 (refused)

```malloy
source: answer is orders_pg.sql("SELECT 7 as n")
```

reason: VERSION_CONFLICT

## Version sales@1.1.5

Above latest (1.1.0) and below the archived 1.2.0: it becomes latest, since a
publish compares with latest, not with the highest version published.

```malloy
source: answer is orders_pg.sql("SELECT 6 as n")
```

## Query latest (again)

Expect:

| n |
| - |
| 6 |

## Version sales@1.3.0

```malloy
source: answer is orders_pg.sql("SELECT 4 as n")
```

## Query latest (again)

Expect:

| n |
| - |
| 4 |
