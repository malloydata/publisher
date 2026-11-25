---
id: an-archived-version-is-refused-until-it-is-unarchived
tags: package-versions, lifecycle
package: sales
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An archived version is refused everywhere until it is unarchived, and latest cannot be archived

Archiving a version takes it out of service: a request that names it is refused
(`VERSION_ARCHIVED`), it cannot become latest, it cannot be published again, and
it leaves memory. Its files stay, so unarchiving puts it straight back in
service. The package's latest cannot be archived: move latest first.

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

## Archive sales@1.1.0 (refused)

reason: VERSION_IS_LATEST

## Query loaded (version=1.0.0)

Loads 1.0.0, so the archive below has a loaded version to unload.

```malloy
run: answer -> { select: n }
```

Expect:

| n |
| - |
| 1 |

## Archive sales@1.0.0

## Query archived (version=1.0.0, refused)

```malloy
run: answer -> { select: n }
```

cites: is archived

## Latest sales@1.0.0 (refused)

reason: VERSION_ARCHIVED

## Version sales@1.0.0 (refused)

Its own content again: refused while archived, rather than quietly re-placed.

```malloy
source: answer is orders_pg.sql("SELECT 1 as n")
```

reason: VERSION_ARCHIVED

## Versions sales

Expect:

| version | latest | archived | loaded |
| ------- | ------ | -------- | ------ |
| 1.1.0   | true   | false    | true   |
| 1.0.0   | false  | true     | false  |

## Unarchive sales@1.0.0

## Query archived (again, version=1.0.0)

Back in service, with the content it was published with.

Expect:

| n |
| - |
| 1 |
