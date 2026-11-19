---
id: persist-shape-not-materializable
tags: eligibility, backstop
package: pd
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Backstop: a `#@ persist` source Malloy drops fails the load, not silently ignored

Malloy's `getBuildTargets()` does not plan a filtered pass-through source
(`X is <table> extend { where … }`) — no target holds it, so the `#@ persist`
annotation would otherwise be a **silent no-op**: no build, no error, served
live. (The underlying fix belongs in Malloy; this scenario guards the
publisher's backstop against the silent drop.)

The publisher must instead fail loudly. A `#@ persist` on a shape Malloy does not
plan is an invalid annotation (`persist-not-materializable`), so the publish and
the load fail on every path. This is the "never a silent
fallback" invariant — every `#@ persist` source is built, refused, or fails the
load.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pd_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 25         |

## Model pd.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.pd_orders')

#@ persist name="pd_high_value" storage=lake
source: high_value is orders extend {
  where: amount >= 100
}
```

## Rejected pd

The package does not load, so it is not served, rather than silently serving
live with no signal.

## Build refused pd

Building the package fails loudly rather than reporting success with an empty
manifest.

cites: is not a source Malloy can materialize

## Build refused pd

Under the load-failure code.

cites: [persist-not-materializable]
