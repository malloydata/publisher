# Row-level access

> What this is: how to restrict **which rows** a caller sees, using [givens](givens.md). This is one
> application of givens; for allowing/denying a whole source see [authorize.md](authorize.md), and for
> the base mechanism see [givens.md](givens.md).

Three related but distinct things live here — keep them apart:

- **Row-level filtering** — a source scopes its own rows by a caller-supplied given. This is a
  convenience and a performance/UX tool (each caller sees only their slice). It is *not*, by itself,
  a security boundary: a caller who omits the given may see everything.
- **Row-level access control** — the same row scoping, made **mandatory** and validated with an
  `#(authorize)` gate, behind a trusted tier. Now a caller *cannot* opt out of their slice, and the
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
it isn't scoped to a tenant, and a caller who supplies a *different* tenant sees that tenant's rows.
To make the scoping a boundary, add a gate.

## Row-level access control

Pair the scoping `where:` with an [`#(authorize)`](authorize.md) gate so the source is queryable only
when a valid scoping value is asserted. An unset or unsatisfied given fails the gate with **HTTP 403**,
so there is no "unscoped" path:

```malloy
##! experimental.givens

given: TENANT :: string

source: orders is duckdb.table('orders.parquet') extend {
  where: tenant = $TENANT
  measure: order_count is count()

  // Deny unless the caller asserts a tenant on the allow-list.
  #(authorize)
  internal dimension: authorized is
    $TENANT = 'acme' or $TENANT = 'globex' or $TENANT = 'initech'
}
```

- `#(authorize)` decides **whether** the caller may query `orders` at all (unset/invalid `TENANT` → 403).
- `where: tenant = $TENANT` decides **which rows** they get once allowed.

Used together, callers can only reach `orders` with a recognized tenant, and only ever see that
tenant's rows.

## Row-level authorize

The pairing above uses two expressions: `#(authorize)` decides **whether** the caller may enter the
source at all, and `where:` decides **which rows** they get once admitted. A gate that references a
row field (see [authorize.md § Row-level gates](authorize.md#row-level-gates)) folds both jobs into
one: instead of a whole-source 403/200 decision, the gate itself becomes the row filter.

```malloy
##! experimental.givens

given: GROUPS :: string[]

source: orders is duckdb.table('orders.parquet') extend {
  measure: order_count is count()

  #(authorize)
  internal dimension: authorized is org_id in $GROUPS
}
```

A caller with `GROUPS: [7, 8]` sees only rows where `org_id` is 7 or 8; a caller with no groups (or
an empty array) sees zero rows — filtered rather than denied, and there is no separate `where:` to
write or to keep in sync.

### Which to reach for

- **`where: field in $GIVEN` alone** — a convenience filter, not access control. A caller who omits
  the given sees everything. Use it when scoping is a UX nicety, not a boundary.
- **`where:` paired with `#(authorize)`** (the pattern above) — the gate is a whole-source boolean
  (admit, or 403); `where:` does the row scoping. Reach for this when the "may enter, but only sees
  their rows" logic genuinely needs two independent expressions — an admin-override gate
  (`$ROLE = 'admin'`) whose row scoping differs from a tenant's (`tenant = $TENANT`), for example —
  or when the row scoping itself is more than a gate dimension can hold (a `filter<T>`, a range, a
  join-based lookup composed across several fields): a gate is exactly one scalar boolean dimension,
  so anything that needs its own named intermediate steps belongs in `where:` instead.
- **A row-level `#(authorize)` gate alone** — when the whole-source decision and the row scope are
  the *same* comparison (`org_id in $GROUPS` is both "may they enter" and "which rows"), write it
  once as a gate. An unset or empty given fails closed to zero rows, with no matching pair of
  expressions that could drift apart.

A gate dimension's expression is otherwise unrestricted — see
[authorize.md § Row-level gates](authorize.md#row-level-gates) for what changed there (there is no
longer a fixed allowlist of accepted comparison shapes) and for the one case where an unsupported
combination now surfaces as a request-time failure instead of a load-time refusal.

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

Neither `ROLE` nor `TENANT` carries a default (a gate-referenced given may not — see
[authorize.md § Row-level gates](authorize.md#row-level-gates)), so every request must send both
keys; the one not on the caller's path is sent blank:

```bash
API=http://localhost:4000/api/v0/environments/examples/packages/governed-analytics/models

# Admin → every tenant
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_tenant","givens":{"ROLE":"admin","TENANT":""}}'  # → 3 tenants

# Tenant caller → only their own rows
curl -s -X POST $API/secured.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_secured -> by_tenant","givens":{"ROLE":"","TENANT":"acme"}}'   # → 1 tenant
```

## Locking the base source

Neither `where:` nor `#(authorize)` is walked through joins — both apply to the source a query
enters through. (A row-level gate on the entry point may *reference* a field on a joined source —
see [authorize.md § entry point](authorize.md#the-entry-point-and-only-the-entry-point) — but that
is not the same as a joined source's own gate firing; the rule here is unchanged.) `#(authorize)`
_is_ carried to an extension that declares no gate of its own, but an extension declaring its OWN
gate replaces it. So two things are yours to get right: which sources a
caller can enter through (anything ungated that joins the base hands the base over), and what each
extension re-exposes. Lock the base with a `false` gate dimension, re-expose curated, separately-gated
extensions with [access modifiers](https://docs.malloydata.dev/documentation/experiments/include),
and do not rely on a join to carry the lock. See
[authorize.md § The entry point, and only the entry point](authorize.md#the-entry-point-and-only-the-entry-point)
and [§ Recommended pattern: locked base and curated extensions](authorize.md#recommended-pattern-locked-base-and-curated-extensions).
