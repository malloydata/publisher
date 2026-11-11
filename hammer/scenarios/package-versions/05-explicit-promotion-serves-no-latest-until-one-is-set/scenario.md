---
id: explicit-promotion-serves-no-latest-until-one-is-set
tags: package-versions, lifecycle
package: sales
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Under explicit promotion, a publish never moves latest; only setting it does

`versionPromotion: explicit` is the mode for an orchestrator that decides when a
version is ready to serve. A published version is servable by name at once, but
a request naming no version has nothing to answer from until latest is set, and
a later publish leaves latest where it is.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_VERSION_PROMOTION: explicit

## Model anchor/anchor.malloy

```malloy
source: anchor is orders_pg.sql("SELECT 0 as n")
```

## Version sales@1.0.0

```malloy
source: answer is orders_pg.sql("SELECT 1 as n")
```

## Query named (version=1.0.0)

```malloy
run: answer -> { select: n }
```

Expect:

| n |
| - |
| 1 |

## Query latest (refused)

```malloy
run: answer -> { select: n }
```

cites: has no latest version

## Latest sales@1.0.0

## Query latest (again)

Expect:

| n |
| - |
| 1 |

## Version sales@1.1.0

```malloy
source: answer is orders_pg.sql("SELECT 2 as n")
```

## Query latest (again)

Still 1.0.0: the publish did not promote.

Expect:

| n |
| - |
| 1 |

## Versions sales

Expect:

| version | latest |
| ------- | ------ |
| 1.1.0   | false  |
| 1.0.0   | true   |
