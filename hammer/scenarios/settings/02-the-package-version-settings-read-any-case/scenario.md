---
id: package-version-settings-any-case
tags: config, settings, package-versions
package: sales
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The package-version settings read any case

`PUBLISHER_VERSION_PROMOTION` is one of a fixed set of values, read in any case
through the settings registry (docs/configuration.md). This holds it end to
end, on a setting fixed at process start: `Explicit` leaves a published version
unpromoted until latest is set (see
`explicit-promotion-serves-no-latest-until-one-is-set`, which uses lower case).

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_VERSION_PROMOTION: Explicit

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

`Explicit` is explicit: the publish did not make the version latest.

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
