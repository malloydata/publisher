---
id: a-package-served-from-its-configuration-is-published-as-its-version
tags: package-versions, lifecycle
package: sales
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A package served from its configuration is published as its version

With package versioning on, a package the server loads with its environment is
published from its location as the version its publisher.json declares, exactly
as a publish from that location would be (the harness writes `1.0.0`). It is the
package's latest, a request naming it is answered, and a version it does not
have is refused rather than answered from another.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_PACKAGE_VERSIONING: on

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

## Query named (version=1.0.0)

```malloy
run: answer -> { select: n }
```

Expect:

| n |
| - |
| 1 |

## Query unknown (version=9.9.9, refused)

```malloy
run: answer -> { select: n }
```

cites: has no version 9.9.9

## Versions sales

Expect:

| version | latest | archived |
| ------- | ------ | -------- |
| 1.0.0   | true   | false    |
