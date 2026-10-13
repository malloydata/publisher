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
the join carries its own inline pipeline, one per custom field, which is how a
host materializes per-tenant fields without a model edit.

The resolver used to anchor the join on the declared source and then insist on
anchoring the source's base as well; an inline `table(...)` has no identity to
anchor on, so the join was refused `403 Access denied for source "cf_333"` —
plainly, with the inline pipeline, and through any derivation of the source. A
query source the model declares is now anchored as itself: its gate entry
already carries its base's gate (query-source derivation is additive), so the
base needs no walk. The filter still lands, per caller, and a request with no
given is still refused.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cj_lists

| org_id:text | list_entry_id:int |
| ----------- | ----------------- |
| acme        | 1                 |
| acme        | 2                 |
| globex      | 3                 |

## Data orders_pg.cj_fact

| org_id:text | list_entry_id:int | entity_attribute_id:int | option_text:text |
| ----------- | ----------------- | ----------------------- | ---------------- |
| acme        | 1                 | 333                     | Hot              |
| acme        | 2                 | 333                     | Cold             |
| globex      | 3                 | 333                     | Warm             |
| acme        | 1                 | 999                     | ignored          |

## Model cj.malloy

```malloy
##! experimental { givens }
given:
  ORG :: string

#(access_filter) org_id = $ORG
source: lists is orders_pg.table('public.cj_lists') extend {
  primary_key: list_entry_id
  measure: company_count is count()
}

// The masking shape: a query source over an anonymous base.
#(access_filter) org_id = $ORG
source: fact is orders_pg.table('public.cj_fact') extend { dimension: nm is option_text }
  -> { select: org_id, list_entry_id, entity_attribute_id, nm } extend { primary_key: list_entry_id }

// A derivation of it: the refusal was transitive.
source: fact_qs is fact -> { group_by: org_id, list_entry_id, nm } extend { primary_key: list_entry_id }
```

## Query the entry point is filtered

The gate on the query source works as a run target; this is the control.

```malloy
run: fact -> { group_by: org_id, nm; order_by: nm asc }
```

givens: ORG=acme

Expect:

| org_id | nm      |
| ------ | ------- |
| acme   | Cold    |
| acme   | Hot     |
| acme   | ignored |

## Query a custom-field join, as acme

The caller's own join, with its own pipeline, into the anonymous-base source.
Admitted, and `fact`'s filter is grafted into it: acme's two lists carry their
values; `globex`'s list is in scope through `lists`' own gate only when the
caller is globex, so it is absent here.

```malloy
run: lists extend {
  join_one: cf_333 is fact -> { where: entity_attribute_id = 333; group_by: org_id, list_entry_id, v is nm }
    on list_entry_id = cf_333.list_entry_id and org_id = cf_333.org_id
  dimension: field_333 is cf_333.v
} -> { group_by: field_333; aggregate: company_count; order_by: field_333 asc }
```

givens: ORG=acme

columns: exact

Expect:

| field_333 | company_count |
| --------- | ------------- |
| Cold      | 1             |
| Hot       | 1             |

## Query a custom-field join, as globex

```malloy
run: lists extend {
  join_one: cf_333 is fact -> { where: entity_attribute_id = 333; group_by: org_id, list_entry_id, v is nm }
    on list_entry_id = cf_333.list_entry_id and org_id = cf_333.org_id
  dimension: field_333 is cf_333.v
} -> { group_by: field_333; aggregate: company_count; order_by: field_333 asc }
```

givens: ORG=globex

Expect:

| field_333 | company_count |
| --------- | ------------- |
| Warm      | 1             |

## Query a plain join to a derivation of it

Transitive: `fact_qs` is a query source over the query source. Same rule.

```malloy
run: lists extend { join_one: f is fact_qs on list_entry_id = f.list_entry_id } -> { group_by: f.nm; aggregate: company_count; order_by: nm asc }
```

givens: ORG=acme

Expect:

| nm      | company_count |
| ------- | ------------- |
| Cold    | 1             |
| Hot     | 1             |
| ignored | 1             |

## Query with no given (refused)

The gate is still a gate: a caller who supplies no `ORG` is refused, naming the
caller's own alias.

```malloy
run: lists extend {
  join_one: cf_333 is fact -> { where: entity_attribute_id = 333; group_by: org_id, list_entry_id, v is nm }
    on list_entry_id = cf_333.list_entry_id and org_id = cf_333.org_id
  dimension: field_333 is cf_333.v
} -> { group_by: field_333; aggregate: company_count }
```

cites: Access denied
