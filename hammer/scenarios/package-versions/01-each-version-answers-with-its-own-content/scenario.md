---
id: each-version-answers-with-its-own-content
tags: package-versions, serve-correctness
package: sales
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Every published version keeps answering with its own content

With package versioning on, a publish from a location publishes an immutable
version, numbered by the `version` in the package's own publisher.json. Each
version keeps serving the content it was published with: a request that names a
version gets that version's answer, a request that names none gets `latest`, and
a version the package never published is refused rather than answered from
another one.

Each version's model answers a different number, so which version answered is
read off the answer.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_PACKAGE_VERSIONING: on

## Model anchor/anchor.malloy

A configured environment needs one package to load at all. This one is
unversioned, and stays so beside the versioned package.

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

## Query second (version=1.1.0)

```malloy
run: answer -> { select: n }
```

Expect:

| n |
| - |
| 2 |

## Query unpublished (version=9.9.9, refused)

```malloy
run: answer -> { select: n }
```

cites: has no version 9.9.9

## Versions sales

Highest first, with latest marked. Both are loaded: each was named above.

Expect:

| version | latest | archived | loaded |
| ------- | ------ | -------- | ------ |
| 1.1.0   | true   | false    | true   |
| 1.0.0   | false  | false    | true   |

## Query anchor (pkg=anchor)

The unversioned package beside it answers as it always did.

```malloy
run: anchor -> { select: n }
```

Expect:

| n |
| - |
| 0 |
