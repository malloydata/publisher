<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Row-level access

> What this is: how to restrict **which rows** a caller sees, using [givens](givens.md). This is one
> application of givens; for gating access with `#(access_filter)` see [authorize.md](authorize.md), and
> for the base mechanism see [givens.md](givens.md).

> **Operating model.** Publisher is a microservice. It runs locally with your own permissions, or
> inside your cloud behind the gateway or application that authenticates your users; it does not
> authenticate anyone itself. The controls on this page are evaluated against what each request
> carries, so they enforce your policy only when that gateway authenticates the user, sets the
> givens from the verified identity, and is the only way to reach Publisher. See
> [the operating model](security-posture.md#operating-model-a-microservice-behind-your-gateway).

Three related but distinct things live here — keep them apart:

- **Row-level filtering** — a source scopes its own rows by a caller-supplied given. This is a
  convenience and a performance/UX tool (each caller sees only their slice). It is _not_, by itself,
  a security boundary: a caller who omits the given may see everything.
- **Row-level access control** — the same row scoping, made **mandatory** and validated with an
  `#(access_filter)` gate, behind a trusted tier. Now a caller _cannot_ opt out of their slice, and the
  scoping value is one the trusted tier asserts from verified identity.
- **Row-level authorize** — the gate itself does the row scoping, instead of pairing it with a
  separate `where:`. See [Row-level authorize](#row-level-authorize) below.

## Row-level filtering

Declare a given and reference it in the source's `where:` so every query against the source is scoped
to the supplied value:

```malloy
##! experimental.givens

#(description="Tenant to scope all rows to")
given: TENANT :: string

source: orders is duckdb.table('orders.parquet') extend {
  where: tenant = $TENANT
  measure: order_count is count()
}
```

Every query against `orders` now returns only the caller's tenant:

```bash
curl -X POST .../models/orders.malloy/query \
  -H 'content-type: application/json' \
  -d '{"query":"run: orders -> { aggregate: order_count }","givens":{"TENANT":"acme"}}'
```

On its own this is filtering, not access control: because `TENANT` has no default, a caller who omits
it isn't scoped to a tenant, and a caller who supplies a _different_ tenant sees that tenant's rows.
To make the scoping a boundary, add a gate.

## Row-level access control

Pair the scoping `where:` with an [`#(access_filter)`](authorize.md) gate so the source is queryable only
when a valid scoping value is asserted, and there is no "unscoped" path. An **unset** `TENANT` is a
**403** (a gate-referenced given may carry no default, so there is nothing to fall back to); a
**recognized-but-unlisted** tenant matches no row and gets **200 with zero rows**. Reach for
[`#(authorize)`](authorize.md) instead when an unlisted tenant should be refused rather than served
an empty result:

```malloy
##! experimental.givens

#(secure)
given: TENANTS :: string[]

// Deny unless the trusted tier resolved the caller to a tenant on the allow-list.
#(access_filter) tenant in $TENANTS
source: orders is duckdb.table('orders.parquet') extend {
  where: tenant in $TENANTS
  measure: order_count is count()
}
```

- `#(access_filter)` decides **whether** the caller may query `orders` at all.
- `where: tenant in $TENANTS` decides **which rows** they get once allowed.

Used together, callers can only reach `orders` with a recognized tenant, and only ever see that
tenant's rows.

## Row-level authorize

The pairing above uses two expressions: `#(access_filter)` decides **whether** the caller may enter the
source at all, and `where:` decides **which rows** they get once admitted. A gate that references a
row field (see [authorize.md § Row-level gates](authorize.md#row-level-gates)) folds both jobs into
one: instead of an all-or-nothing admit decision, the gate itself becomes the row filter.

```malloy
##! experimental.givens

#(secure)
given: GROUPS :: string[]

#(access_filter) org_id in $GROUPS
source: orders is duckdb.table('orders.parquet') extend {
  measure: order_count is count()
}
```

A caller with `GROUPS: [7, 8]` sees only rows where `org_id` is 7 or 8; a caller with no groups (or
an empty array) sees zero rows — filtered rather than denied, and there is no separate `where:` to
write or to keep in sync.

### Which to reach for

- **`where: field in $GIVEN` alone** — a convenience filter, not access control. A caller who omits
  the given sees everything. Use it when scoping is a UX nicety, not a boundary.
- **`where:` paired with `#(access_filter)`** (the pattern above) — reach for this when the row scope
  needs more than a single gate term can hold (a `filter<T>`, a range, a join-based lookup composed
  across several fields, an `and` of terms over more givens than the gate references): a gate's body
  is a narrow grammar of `and`-joined terms (see [authorize.md § Expression
  Language](authorize.md#expression-language)), so anything needing its own named intermediate steps
  or a comparison the grammar does not accept belongs in `where:` instead, with the gate covering
  only the admit/deny decision.
- **A row-level `#(access_filter)` gate alone** — when the access decision and the row scope are
  the _same_ comparison (`org_id in $GROUPS` is both "may they enter" and "which rows"), write it
  once as a gate. An unset or empty given fails closed to zero rows, with no matching pair of
  expressions that could drift apart.

A gate's expression follows the narrow grammar in
[authorize.md § Expression Language](authorize.md#expression-language) — not the full Malloy
expression language `where:` accepts.

> **Trusted-tier requirement.** Givens are **caller-asserted** — anyone who can reach the query API
> can send `{"TENANTS":["acme"]}`. Row-level access control is a real boundary only when Publisher sits
> behind a trusted tier that authenticates the end user and sets `TENANTS` from its own verified
> context, with the query/MCP API network-isolated from untrusted callers. See
> [authorize.md § Security model](authorize.md#security-model) for the full deployment contract.
> Identity-bound givens (values the caller cannot override) are a planned milestone.

## Where a gate is evaluated

A row-level gate is grafted into the query it scopes and evaluated with that query. Two
consequences follow for an author choosing what to gate on. Neither is a defect: for both of these
live behaviours, Publisher's served answer matches a live run, because this is how gates are
evaluated.

### A time-based gate is evaluated in the query's time zone

> **Know this.** A gate on a field computed from a timestamp is evaluated in the query's
> `timezone:`, and a query may choose its own zone.

```malloy
##! experimental.givens

#(secure)
given: ON_DAY :: date

#(access_filter) dday = $ON_DAY
source: events is duckdb.table('events.parquet') extend {
  dimension: dday is ts.day
}
```

Malloy computes `ts.day`, and the `where:` the gate grafts, in the query's time zone. A query that
sets a different `timezone:` moves the day boundary, so the rows the gate admits for one `ON_DAY`
shift by that zone's offset: up to 14 hours either side of midnight.

When the gate is an access boundary:

- Gate on a column that already holds the date or key (a `date`-typed column or a `tenant_id`),
  not a timestamp truncated in a query stage or an `extend` dimension.
- Or make sure the rows either side of the boundary, within that window, belong to the same tenant,
  so a shifted day admits nothing the caller could not already see.

### An inherited gate filters a derivation's input

A `->` derivation over a gated source carries the base's gate (see
[authorize.md § The entry point](authorize.md#the-entry-point-and-only-the-entry-point)), and that
gate filters the base's rows before the derivation runs. Everything the derivation computes, it
computes over only the rows the gate admits for the caller.

```malloy
##! experimental.givens

#(secure)
given: REGION :: string

#(access_filter) region = $REGION
source: sales is duckdb.table('sales.parquet') extend {
  measure: total is amount.sum()
}

source: shares is sales -> {
  group_by: region, category
  aggregate:
    total
    share is total / all(total)
}
```

A caller of `shares` sees their own region's rows, and `share` is each row's share of their region's
total. The same holds for `exclude()`, window functions, `calculate:` and `limit`: a top-N ranks the
caller's rows. A derivation that renames a column to the gate's field name
(`group_by: region is category`) does not change what the gate reads: it reads the base's `region`.
A derivation need not keep the gate's column at all; the rows are filtered before it groups them.
Chains are the same: in `y is shares -> { … }`, the gate filters `sales` inside `shares` inside `y`.

A gate the derivation declares on itself (`#(access_filter) region = $REGION` above `source: shares
is sales -> { … }`) filters the derivation's own output, after its query has run over every row of
its base. Use it only when that is the intent: its `all()`, windows and `limit` see every row.

A derivation whose base hides the gate's column (`sales extend { except: region } -> { … }`) gives
the gate nothing to filter, so every query against it is denied.

## Runnable example

[`examples/governed-analytics`](../examples/governed-analytics) implements the row-level authorize
pattern above in [`secured.malloy`](../examples/governed-analytics/secured.malloy): the
`#(access_filter) tenant in $TENANTS` gate on `orders_secured` is both the admit decision and the row
scope, with no separate `where:`. It ships in the default `examples` environment, so against the
running example the same query returns different rows per caller:

`TENANTS` carries no default (a gate-referenced given may not — see
[authorize.md § Row-level gates](authorize.md#row-level-gates)), so every request must send it.
There is no separate admin role: a caller whose identity resolves to every tenant on the list is
simply handed all of them by the trusted tier that sets `TENANTS`.

```bash
API=http://localhost:4000/api/v0/environments/examples/packages/governed-analytics/models

# Resolved to every tenant → sees every tenant
curl -s -X POST $API/index.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_tenant","givens":{"TENANTS":["acme","globex","initech"]}}'  # → 3 tenants

# Resolved to one tenant → only their own rows
curl -s -X POST $API/index.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_tenant","givens":{"TENANTS":["acme"]}}'   # → 1 tenant
```

## Locking the base source

Neither `where:` nor `#(access_filter)` is walked through joins — both apply to the source a query
enters through. (A row-level gate on the entry point may _reference_ a field on a joined source —
see [authorize.md § entry point](authorize.md#the-entry-point-and-only-the-entry-point) — but that
is not the same as a joined source's own gate firing; the rule here is unchanged.) `#(access_filter)`
_is_ carried to an extension that declares no gate of its own, but an extension declaring its OWN
gate replaces it. So two things are yours to get right: which sources a
caller can enter through (anything ungated that joins the base hands the base over), and what each
extension re-exposes. Lock the base with `#(access_filter) false`, re-expose curated, separately-gated
extensions with [access modifiers](https://docs.malloydata.dev/documentation/experiments/include),
and do not rely on a join to carry the lock. See
[authorize.md § The entry point, and only the entry point](authorize.md#the-entry-point-and-only-the-entry-point)
and [§ Recommended pattern: locked base and curated extensions](authorize.md#recommended-pattern-locked-base-and-curated-extensions).
