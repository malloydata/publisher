---
id: a-caller-join-to-a-gated-query-source-over-an-anonymous-base
tags: security, caller-joins, authorize
package: cj
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A caller join to a gated query source over an anonymous base is admitted and filtered

A join written in query text is the caller entering the joined source, so that
source's `#(access_filter)` is grafted into the join with the caller's givens.
Here the joined source is the common masking shape — a query source over an
**anonymous** base, `table(...) extend { … } -> { select … } extend { … }` — and
the join carries its own inline pipeline, one per attribute, which is how a host
turns a long attribute table into per-tenant columns without a model edit.

The resolver used to anchor the join on the declared source and then insist on
anchoring the source's base as well; an inline `table(...)` has no identity to
anchor on, so the join was refused `403 Access denied for source "<alias>"` —
plainly, with the inline pipeline, and through any derivation of the source. A
query source the model declares is now anchored as itself: its gate entry
already carries its base's gate (query-source derivation is additive), so the
base needs no walk. The filter still lands, per caller, and a request with no
given is still refused.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cj_projects

| tenant_id:text | project_id:int |
| -------------- | -------------- |
| north          | 1              |
| north          | 2              |
| south          | 3              |

## Data orders_pg.cj_attr_values

| tenant_id:text | project_id:int | attr_id:int | label:text |
| -------------- | -------------- | ----------- | ---------- |
| north          | 1              | 7           | High       |
| north          | 2              | 7           | Low        |
| south          | 3              | 7           | Medium     |
| north          | 1              | 9           | Other      |

## Model cj.malloy

```malloy
##! experimental { givens }
given:
  TENANT :: string

#(access_filter) tenant_id = $TENANT
source: projects is orders_pg.table('public.cj_projects') extend {
  primary_key: project_id
  measure: project_count is count()
}

// The masking shape: a query source over an anonymous base.
#(access_filter) tenant_id = $TENANT
source: attr_values is orders_pg.table('public.cj_attr_values') extend { dimension: lbl is label }
  -> { select: tenant_id, project_id, attr_id, lbl } extend { primary_key: project_id }

// A derivation of it: the refusal was transitive.
source: attr_rollup is attr_values -> { group_by: tenant_id, project_id, lbl } extend { primary_key: project_id }
```

## Query the entry point is filtered

The gate on the query source works as a run target; this is the control.

```malloy
run: attr_values -> { group_by: tenant_id, lbl; order_by: lbl asc }
```

givens: TENANT=north

Expect:

| tenant_id | lbl     |
| --------- | ------- |
| north     | High    |
| north     | Low     |
| north     | Other   |

## Query an attribute join, as north

The caller's own join, with its own pipeline, into the anonymous-base source.
Admitted, and `attr_values`' filter is grafted into it: north's two projects
carry their values; south's project is in scope through `projects`' own gate
only when the caller is south, so it is absent here.

```malloy
run: projects extend {
  join_one: priority is attr_values -> { where: attr_id = 7; group_by: tenant_id, project_id, v is lbl }
    on project_id = priority.project_id and tenant_id = priority.tenant_id
  dimension: priority_label is priority.v
} -> { group_by: priority_label; aggregate: project_count; order_by: priority_label asc }
```

givens: TENANT=north

columns: exact

Expect:

| priority_label | project_count |
| -------------- | ------------- |
| High           | 1             |
| Low            | 1             |

## Query an attribute join, as south

```malloy
run: projects extend {
  join_one: priority is attr_values -> { where: attr_id = 7; group_by: tenant_id, project_id, v is lbl }
    on project_id = priority.project_id and tenant_id = priority.tenant_id
  dimension: priority_label is priority.v
} -> { group_by: priority_label; aggregate: project_count; order_by: priority_label asc }
```

givens: TENANT=south

Expect:

| priority_label | project_count |
| -------------- | ------------- |
| Medium         | 1             |

## Query a plain join to a derivation of it

Transitive: `attr_rollup` is a query source over the query source. Same rule.

```malloy
run: projects extend { join_one: a is attr_rollup on project_id = a.project_id } -> { group_by: a.lbl; aggregate: project_count; order_by: lbl asc }
```

givens: TENANT=north

Expect:

| lbl     | project_count |
| ------- | ------------- |
| High    | 1             |
| Low     | 1             |
| Other   | 1             |

## Query with no given (refused)

The gate is still a gate: a caller who supplies no `TENANT` is refused, naming
the caller's own alias.

```malloy
run: projects extend {
  join_one: priority is attr_values -> { where: attr_id = 7; group_by: tenant_id, project_id, v is lbl }
    on project_id = priority.project_id and tenant_id = priority.tenant_id
  dimension: priority_label is priority.v
} -> { group_by: priority_label; aggregate: project_count }
```

cites: Access denied
