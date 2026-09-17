---
id: given-in-a-persisted-query-is-refused
tags: eligibility, security, givens
package: gpq
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A given is honoured per caller or the source is refused, never frozen at its default

Where a given sits decides whether each caller gets their own rows or everyone
gets the author's default. Inside the persisted query the compiler has only one
value available at build time — the declaration default — so it is substituted
and the table holds that one caller's slice; the read path swaps only the `FROM`,
and nothing re-applies a filter that lives inside the relation. In the source's
extend block the given is absent from the build and applied over the materialized
rows at read, with the value each caller supplies.

The first shape has no safe reading, so it is refused at build. The second is the
documented form (`docs/row-level-access.md`) and must keep working — including
FROM the artifact, which is the half a refusal alone would not prove.

Three packages, because a refusal fails its build: `gpq` carries the refused
shape written directly, `gpqa` the same substitution reached through a source
argument, and `gpqb` the honoured one.

Without a default the first shape cannot build at all (`Given 'ORG_ID' has no
value and no default`), so this needs the default to exist.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.gpq_rows

| org_id:int | amount:num |
| ---------- | ---------- |
| 1          | 100        |
| 2          | 50         |
| 1          | 25         |

## Model gpq/gpq.malloy

The given is inside the persisted query, so the build would substitute `1`.

```malloy
##! experimental { persistence givens }
given: ORG_ID :: number is 1

source: raw is orders_pg.sql('SELECT org_id, amount FROM public.gpq_rows')

#@ persist name="gpq_inside"
source: inside is raw -> { where: org_id = $ORG_ID; select: * }
```

## Model gpqa/gpqa.malloy

The same substitution reached a different way: the given is bound as a source
ARGUMENT, so it never appears as a reference in the query and the build bakes it
anyway. Refused for the same reason.

```malloy
##! experimental { persistence givens parameters }
given: ORG_ID :: number is 1

source: raw is orders_pg.sql('SELECT org_id, amount FROM public.gpq_rows')
source: scoped(x::number) is raw extend { where: org_id = x }

#@ persist name="gpqa_arg"
source: arg_bound is scoped(x is $ORG_ID) -> { select: * }
```

## Model gpqb/gpqb.malloy

The same given, moved to the extend block.

```malloy
##! experimental { persistence givens }
given: ORG_ID :: number is 1

source: raw is orders_pg.sql('SELECT org_id, amount FROM public.gpq_rows')

#@ persist name="gpqb_outside"
source: outside is raw -> { select: * } extend { where: org_id = $ORG_ID }
```

## Build refused gpq

Refused rather than built, and the message names the placement that fixes it —
the safe shape is one move away from the refused one.

cites: persisted query references a given

## Build refused gpqa

cites: persisted query references a given

## Publish gpqb

## Query outside default (pkg=gpqb)

No given supplied, so the declaration default applies: org 1 is 100 + 25.

```malloy
run: outside -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 125       |

## Query outside for org 2 (pkg=gpqb)

givens: ORG_ID=2

The same source, a different caller. `125` here would mean the default had been
frozen into the table and served to everyone.

```malloy
run: outside -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 50        |

## Mutate orders_pg.gpq_rows

| org_id:int | amount:num |
| ---------- | ---------- |
| 1          | 1000       |

## Query outside default (again, pkg=gpqb)

Stale `125` ⇒ answered from the materialized rows with the given applied over
them. Both halves matter: a fresh `1125` would mean the artifact was never read,
and the per-caller answer above would prove nothing about serving.

```malloy
run: outside -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 125       |
