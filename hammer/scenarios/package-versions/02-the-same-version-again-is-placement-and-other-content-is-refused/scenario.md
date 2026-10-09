---
id: the-same-version-again-is-placement-and-other-content-is-refused
tags: package-versions, lifecycle
package: sales
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Publishing a version again is placement; publishing other content under it is refused

A published version is immutable. Publishing the same version with the same
content again succeeds and changes nothing, so an orchestrator re-loading a
version onto a server that already holds it can simply retry. Publishing
different content under a version the package already has is refused
(`VERSION_CONFLICT`): the fix is to bump `version` in publisher.json. A
`version` that is not a semantic version is refused before anything is
published.

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

## Version sales@1.0.0

The same content again: placement, not a second publish.

```malloy
source: answer is orders_pg.sql("SELECT 1 as n")
```

## Version sales@1.0.0 (refused)

Different content under the same version.

```malloy
source: answer is orders_pg.sql("SELECT 5 as n")
```

reason: VERSION_CONFLICT

## Version sales@banana (refused)

```malloy
source: answer is orders_pg.sql("SELECT 6 as n")
```

reason: MANIFEST_VERSION_INVALID

## Query latest

Still the content first published under 1.0.0.

```malloy
run: answer -> { select: n }
```

Expect:

| n |
| - |
| 1 |

## Versions sales

Expect:

| version | latest | archived |
| ------- | ------ | -------- |
| 1.0.0   | true   | false    |
