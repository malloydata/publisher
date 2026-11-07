---
id: latest-rolls-back-and-a-higher-version-moves-it-forward
tags: package-versions, lifecycle
package: sales
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Pointing latest at an earlier version rolls back, and the next higher publish moves it forward

`latest` is the version a request naming none is served from. Pointing it at an
earlier version is a rollback: nothing is rebuilt or fetched, the pointer moves,
and the version that stopped being latest leaves memory (it loads again when
next named). The next publish of a version higher than the current latest
moves latest forward again. `latest` can only point at a version the package
has.

## Publisher

- PERSIST_STORAGE_MODE: on

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

## Latest sales@1.0.0

## Query latest

```malloy
run: answer -> { select: n }
```

Expect:

| n |
| - |
| 1 |

## Versions sales

1.1.0 stopped being latest; it is still published, and stays loaded.

Expect:

| version | latest | loaded |
| ------- | ------ | ------ |
| 1.1.0   | false  | true   |
| 1.0.0   | true   | true   |

## Version sales@1.0.5

Above the rolled-back latest and below 1.1.0: it becomes latest, because a
publish compares with latest, not with the highest version published.

```malloy
source: answer is orders_pg.sql("SELECT 5 as n")
```

## Query latest (again)

Expect:

| n |
| - |
| 5 |

## Version sales@1.2.0

Higher than latest, so it becomes latest.

```malloy
source: answer is orders_pg.sql("SELECT 3 as n")
```

## Query latest (again)

Expect:

| n |
| - |
| 3 |

## Latest sales@9.9.9 (refused)

reason: VERSION_NOT_FOUND
