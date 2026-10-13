---
id: a-gated-source-joined-inside-a-diamond-import-loads
tags: security, authorize, imports
package: dj
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A gated source joined inside a diamond import loads, and its gate still holds

`dims.malloy` declares a locked dimension. `facts.malloy` imports it and
author-joins it inside a named base. `security.malloy` imports both
`dims.malloy` and `facts.malloy` — the diamond — and the entry model imports all
three on top (imports are not transitive, so it imports `dims.malloy` itself
for the given the gates read). Every file in the package is
loaded as a model, so the locked dimension is compiled once per import path,
and the join inside the base embeds one parse's gate note while the namespace
holds the other's.

The load-time check that refuses a gate typed on a join line used to tell a
copied gate from a typed one by object identity alone, so this package failed
to load with `An authorize annotation is never enforced at: on field "field" of
source "fact_base"`. A copied gate is also a note parsed from the gated
source's own `source:` line, so position now counts as well, and the package
loads. The gates themselves are unchanged: the locked dimension is still
refused as a run target, and the filtered source still filters.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.dj_t

| id:int | name:text | region:text |
| ------ | --------- | ----------- |
| 1      | a         | US          |
| 2      | b         | EU          |

## Model dj.malloy

```malloy
import "dims.malloy"
import "security.malloy"
import "facts.malloy"
##! experimental { givens }

source: report is fact extend { measure: n is count() }
```

## Model dims.malloy

```malloy
##! experimental { givens }
given:
  REGION :: string

#(authorize) false
source: dim_attr is orders_pg.table('public.dj_t') extend { rename: attr_id is id; primary_key: attr_id; dimension: attr_name is name }
```

## Model facts.malloy

```malloy
import "dims.malloy"
##! experimental { givens }

#(access_filter) region = $REGION
source: fact_base is orders_pg.table('public.dj_t') extend {
  join_one: field is dim_attr on id = field.attr_id
  where: field.attr_name is null or field.attr_name != 'zzz'
}

#(access_filter) region = $REGION
source: fact is fact_base -> { select: id, region, nm is field.attr_name } extend { primary_key: id }
```

## Model security.malloy

```malloy
import "dims.malloy"
import "facts.malloy"
##! experimental { givens }

#(authorize) true
#(access_filter) region = $REGION
source: secure_attr is dim_attr extend { join_many: values is fact on attr_id = values.id }
```

## Query the filtered source still filters

```malloy
run: report -> { group_by: region; aggregate: n }
```

givens: REGION=US

Expect:

| region | n |
| ------ | - |
| US     | 1 |

## Query the re-opened extension serves

```malloy
run: secure_attr -> { group_by: attr_name; order_by: attr_name asc }
```

givens: REGION=US

Expect:

| attr_name |
| --------- |
| a         |

## Query the locked dimension is still refused (refused)

```malloy
run: dim_attr -> { group_by: attr_name }
```

givens: REGION=US

cites: Access denied
