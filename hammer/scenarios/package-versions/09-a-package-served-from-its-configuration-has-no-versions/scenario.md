---
id: a-package-served-from-its-configuration-has-no-versions
tags: package-versions, lifecycle
package: sales
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A package served from its configuration has no versions

Only a publish from a location makes versions. A package the server loads with
its environment is the single slot it always was: it serves its one tree, it has
no published versions, and a request naming a version is refused rather than
answered from that tree.

## Publisher

- PERSIST_STORAGE_MODE: on

## Model sales/sales.malloy

```malloy
source: answer is orders_pg.sql("SELECT 1 as n")
```

## Query latest

```malloy
run: answer -> { select: n }
```

Expect:

| n |
| - |
| 1 |

## Query named (version=1.0.0, refused)

```malloy
run: answer -> { select: n }
```

cites: has no version 1.0.0

## Versions sales

Expect:

| version |
| ------- |
