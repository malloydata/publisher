---
id: preaggregate-hidden-field-warns-and-loads
tags: preaggregation, access-control, warnings
package: ph
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A rollup on a hidden measure is not built, the package still loads, and it says so

Three claims that only hold together.

**Nothing is built.** A rollup stores each measure's partial, and the stored table is
served under the base's name with none of the source's field visibility applying to it —
so a rollup on a hidden measure would publish what the source hides. The planner refuses
to plan one, which is what makes that true regardless of who reads any message. (Pinned
directly in `preaggregation_synthesis.spec.ts`; here it shows as the destination never
being created at all, since nothing was ever written to it.)

**The package still loads.** Before this rule existed such a package loaded, so making
the violation fatal would stop an already-published package from loading because a server
was upgraded. That is not something an upgrade may do, and it is affordable here
precisely because the planner skip above is the enforcement rather than the refusal.

**And it says so on `/status`.** Which is the claim worth a scenario: a warning nobody
can see is not a warning, and "loads but does nothing, silently" is a worse outcome than
either failing or working. A publish is still refused — this is the load path only.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ph_orders

| order_id:int | category:text | amount:num |
| ------------ | ------------- | ---------- |
| 1            | books         | 100        |
| 2            | books         | 50         |
| 3            | tools         | 200        |

## Model ph.malloy

```malloy
##! experimental { persistence composite_sources access_modifiers }

source: orders is orders_pg.table('public.ph_orders') extend {
  measure:
    #@ preaggregate grain="category" storage=lake
    total is amount.sum()
} include {
  public: category, amount, order_id
  private: total
}
```

## Publish

The package loads. If the violation were fatal this step would fail outright.

## Warns ph

cites: does not publicly expose

## Query by category

`total` is private, so the live path refuses it — which is the behaviour the rollup
would otherwise have quietly overridden. Group by the public dimension and aggregate a
public expression instead, to show the package is serving normally.

```malloy
run: orders -> { group_by: category; aggregate: amt is amount.sum(); order_by: category asc }
```

Expect:

| category | amt |
| -------- | --- |
| books    | 150 |
| tools    | 200 |
