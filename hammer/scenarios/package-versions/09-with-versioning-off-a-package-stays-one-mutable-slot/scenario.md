---
id: with-versioning-off-a-package-stays-one-mutable-slot
tags: package-versions, lifecycle
package: sales
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# With versioning off, a package stays the single mutable slot it always was

Package versioning ships off. Off, a publish from a location installs the
package in place whatever its publisher.json says about a version, publishing
again replaces its content, the package has no published versions, and a request
naming a version is refused rather than answered from the package's one tree.

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

cites: has no published versions

## Version sales@1.0.0

Different content under the same version: off, that is a replacement.

```malloy
source: answer is orders_pg.sql("SELECT 5 as n")
```

## Query latest (again)

Expect:

| n |
| - |
| 5 |

## Versions sales

Expect:

| version |
| ------- |
