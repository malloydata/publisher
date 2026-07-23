---
id: persist-shape-not-materializable
tags: eligibility, backstop
package: pd
---

# Backstop: a `#@ persist` source Malloy drops is refused, not silently ignored

Malloy's `getBuildPlan()` does not recognize a filtered pass-through source
(`X is <table> extend { where … }`) as a materializable build root — it returns
no graph for it, so the `#@ persist` annotation would otherwise be a **silent
no-op**: no build, no error, served live. (The underlying fix belongs in Malloy;
this scenario guards the publisher's backstop against the silent drop.)

The publisher must instead refuse loudly: surface an operator warning at load,
and fail the build. This is the "never a silent fallback" invariant — every
`#@ persist` source is either built or explicitly refused.

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

## Warns pd

The package loads and serves, but surfaces an operator warning that the persist
annotation was not honored (rather than silently serving live with no signal).

cites: not recognized as a materializable source

## Build refused pd

Building the package fails loudly rather than reporting success with an empty
manifest.

cites: not recognized as a materializable source
