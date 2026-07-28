---
id: host-binding-honors-row-level-access
tags: security, orchestration
package: hbi
---

# A host-supplied binding must not bypass row-level access control

A `given` binds per query — it is row-level access control. The build gate therefore
refuses to materialize a given-referencing source (`givens-refused` pins that), because
one table built once and served to everyone hands every caller the rows that were
filtered for whoever built it.

**The rule: that guarantee must not depend on the host getting its manifest right.**
A host is authoritative about *which table backs which source* — it owns generations
and rollout. It cannot be authoritative about *whether a source may be served from a
frozen table at all*, because that is decided by compiling the model, which the host
does not do. So a binding for a source the publisher would refuse to build must not
be honored; that source degrades to serving live.

A manifest can name such a source without anyone being careless: a source that was
given-free when it was built acquires a `given` on the next model edit, and the old
manifest still points at a real, correct table until convergence catches up.

This scenario builds a legitimate given-FREE source, then binds the given-FILTERED
source to that same real table and queries it with `REGION=US`. A live serve returns
US only.

The binding is refused and the source degrades to live. Eligibility is decided once
at compile — the only place the compiled sources exist — and consulted here, so the
bind path enforces it without the recompile the tier-split exists to avoid. A refused
binding is dropped rather than fatal: that source serves live, which is always
correct, and every other binding in the manifest still applies.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.hbi_orders

| order_id:int | region:text | amount:num |
| ------------ | ----------- | ---------- |
| 1            | US          | 100        |
| 2            | EU          | 200        |
| 3            | US          | 50         |

## Model hbi.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given: REGION :: filter<string> is f'US'

source: base is orders_pg.table('public.hbi_orders')

// Eligible: no given anywhere in its lineage. This one really materializes.
#@ persist name="hbi_all" storage=lake
source: all_rollup is base -> {
  group_by: region
  aggregate: t is amount.sum()
}

// INELIGIBLE: row-level filtered by a given. Annotated so it is PLANNED (it has a
// serve handle a host could name), but the eligibility gate refuses to build it —
// `givens-refused` pins that refusal. No honest manifest can carry this source.
source: scoped is base extend {
  where: region ~ $REGION
}

#@ persist name="hbi_scoped" storage=lake
source: scoped_rollup is scoped -> {
  group_by: region
  aggregate: t is amount.sum()
}
```

## Publish (sources=all_rollup)

Build ONLY the eligible source. `scoped_rollup` stays planned-but-unbuilt — building
it is exactly what the eligibility gate refuses, so the run would fail.

expect binding: all_rollup -> lake

## Query the eligible source

The legitimate source serves from storage — both regions, unfiltered, which is
correct for a source with no given in its lineage.

```malloy
run: all_rollup -> { select: region, t; order_by: region asc }
```

Expect:

| region | t   |
| ------ | --- |
| EU     | 200 |
| US     | 150 |

## Manifest hbi

The host authors a manifest vouching for `scoped_rollup` — the given-referencing
source the build gate refuses — at the real `hbi_all` table, and binds it.

- scoped_rollup -> hbi_all @ lake

## Query the given-filtered source

The question this scenario exists to answer. `scoped_rollup` is filtered to
`$REGION`, so a live serve returns US only (150).

If the forged binding is honored, the query is answered from the `hbi_all` table —
which holds EVERY region — and the given's filter is gone for every caller.

```malloy
run: scoped_rollup -> { select: region, t; order_by: region asc }
```

givens: REGION=US

Expect:

| region | t   |
| ------ | --- |
| US     | 150 |

## Note (since=2026-07-25)

> Was a confirmed bypass before the bind-time eligibility check: querying with
> `REGION=US` returned `[{EU,200},{US,150}]` from the stored table. Scope, so the
> fix is not read as broader than it is:
> `#(authorize)` is NOT affected — it is
> evaluated on the original model surface before routing is chosen
> (`model.ts` `assertAuthorized`, then `assertAuthorizedForAllSources`) — and a
> non-portable shape self-corrects through the fallback ladder. GIVENS are the gap:
> the serve transform has no `given` handling at all, and the storage serve path
> deliberately supplies no given values, so nothing downstream can re-impose the
> filter once a binding is accepted.
