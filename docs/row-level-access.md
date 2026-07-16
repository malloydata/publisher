# Row-level access

> What this is: how to restrict **which rows** a caller sees, using [givens](givens.md). This is one
> application of givens; for allowing/denying a whole source see [authorize.md](authorize.md), and for
> the base mechanism see [givens.md](givens.md).

Two related but distinct things live here — keep them apart:

- **Row-level filtering** — a source scopes its own rows by a caller-supplied given. This is a
  convenience and a performance/UX tool (each caller sees only their slice). It is *not*, by itself,
  a security boundary: a caller who omits the given may see everything.
- **Row-level access control** — the same row scoping, made **mandatory** and validated with an
  `#(authorize)` gate, behind a trusted tier. Now a caller *cannot* opt out of their slice, and the
  scoping value is one the trusted tier asserts from verified identity.

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
it isn't scoped to a tenant, and a caller who supplies a *different* tenant sees that tenant's rows.
To make the scoping a boundary, add a gate.

## Row-level access control

Pair the scoping `where:` with an [`#(authorize)`](authorize.md) gate so the source is queryable only
when a valid scoping value is asserted. An unset or unsatisfied given fails the gate with **HTTP 403**,
so there is no "unscoped" path:

```malloy
##! experimental.givens

given: TENANT :: string

// Deny unless the caller asserts a tenant on the allow-list.
#(authorize) "$TENANT in ['acme', 'globex', 'initech']"
source: orders is duckdb.table('orders.parquet') extend {
  where: tenant = $TENANT
  measure: order_count is count()
}
```

- `#(authorize)` decides **whether** the caller may query `orders` at all (unset/invalid `TENANT` → 403).
- `where: tenant = $TENANT` decides **which rows** they get once allowed.

Used together, callers can only reach `orders` with a recognized tenant, and only ever see that
tenant's rows.

> **Trusted-tier requirement.** Givens are **caller-asserted** — anyone who can reach the query API
> can send `{"TENANT":"acme"}`. Row-level access control is a real boundary only when Publisher sits
> behind a trusted tier that authenticates the end user and sets `TENANT` from its own verified
> context, with the query/MCP API network-isolated from untrusted callers. See
> [authorize.md § Security model](authorize.md#security-model) for the full deployment contract.
> Identity-bound givens (values the caller cannot override) are a planned milestone.

## Runnable example

[`examples/governed-analytics`](../examples/governed-analytics) implements exactly this pattern in
[`secured.malloy`](../examples/governed-analytics/secured.malloy): `orders_secured` is gated with
`#(authorize)` and scoped with `where: $ROLE = 'admin' or tenant = $TENANT`. It ships in the default
`examples` environment, so against the running example the same query returns different rows per caller:

```bash
API=http://localhost:4000/api/v0/environments/examples/packages/governed-analytics/models

# Admin → every tenant
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_tenant","givens":{"ROLE":"admin"}}'   # → 3 tenants

# Tenant caller → only their own rows
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_tenant","givens":{"TENANT":"acme"}}'  # → 1 tenant
```

## Locking the base source

`where:`/`#(authorize)` guard the source a query runs against, but neither is inherited through
`extend` or walked through joins. To keep a base table's rows from leaking through an unguarded
extension or join, lock the base with `#(authorize) "false"` and re-expose curated, separately-gated
extensions with [access modifiers](https://docs.malloydata.dev/documentation/experiments/include). See
[authorize.md § Recommended pattern: locked base and curated extensions](authorize.md#recommended-pattern-locked-base-and-curated-extensions).
