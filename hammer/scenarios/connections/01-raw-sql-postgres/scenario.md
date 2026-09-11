---
id: raw-sql-postgres
tags: connections
package: rawsql
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Raw SQL through `sqlQuery` returns rows on Postgres

`POST /connections/<c>/sqlQuery` runs a caller's statement through the
connection's Malloy connector, and on Postgres that connector unwraps a `row`
column from every row it yields — the contract it has with the Postgres
dialect, whose `sqlFinalStage` projects exactly that column. A statement the
caller wrote does not project it, so the driver hands back nothing readable and
`SELECT 1` fails. The endpoint finalizes an eligible statement on the way in to
close that gap.

The suite reaches this endpoint elsewhere only through DuckDB and DuckLake
connections, whose dialect has no final stage and so never needed it. Nothing
pushed a row through the Postgres streaming branch, which is how the byte cap
came to raise `The "string" argument must be of type string or an instance of
Buffer or ArrayBuffer. Received undefined` on every plain statement without
anyone noticing.

## Data orders_pg.rawsql_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-02      | EU          | 200        |

## Model rawsql.malloy

A package so the environment exists; the connection endpoints are what this
scenario exercises, not the model.

```malloy
source: orders is orders_pg.table('public.rawsql_orders')
```

## Connection orders_pg

The reported statement: no table, no `row` column, nothing to unwrap.

```sql
SELECT 1 AS x
```

Expect:

| x   |
| --- |
| 1   |

## Connection orders_pg

A real projection reads back by column name, so the wrapper is transparent
rather than merely non-fatal.

```sql
SELECT region, amount FROM rawsql_orders ORDER BY order_id
```

Expect:

| region | amount |
| ------ | ------ |
| US     | 100    |
| EU     | 200    |

## Connection orders_pg

A CTE, which the wrapper nests inside its own.

```sql
WITH totals AS (SELECT region, SUM(amount) AS total FROM rawsql_orders GROUP BY region)
SELECT region, total FROM totals ORDER BY region
```

Expect:

| region | total |
| ------ | ----- |
| EU     | 200   |
| US     | 100   |

## Connection orders_pg

The transport contract, which is the one that must NOT be finalized. A
`publisher` proxy connection reports the REMOTE connection's dialect, so the
Malloy compiler on the far side finalizes the statement before it is sent and
what arrives here is the shape below -- `@malloydata/db-publisher` forwards it
verbatim and does no unwrapping of its own. Passed through, the connector's
unwrap yields the plain columns the caller compiled for. Finalized a second
time it would yield `{"row": {...}}`: not an error, just nulls read off a level
nothing strips. The Malloy CLI and the VS Code extensions reach a Publisher
this way, and their versions are not ours to coordinate.

```sql
WITH __stage0 AS (SELECT region, amount FROM rawsql_orders WHERE order_id = 1)
SELECT row_to_json(finalStage) as row FROM __stage0 AS finalStage
```

Expect:

| region | amount |
| ------ | ------ |
| US     | 100    |

## Connection orders_pg (rows=0)

DDL is passed through, not finalized: it cannot sit in the subquery position
the wrapper puts a statement in, and it returns no rows for the connector to
unwrap, so it already worked. Wrapping it would turn a working statement into
a syntax error.

```sql
CREATE TABLE rawsql_ddl_probe (a int)
```

## Connection orders_pg

The table the step above created — proof the DDL ran rather than being quietly
swallowed.

```sql
SELECT count(*) AS n FROM information_schema.tables WHERE table_name = 'rawsql_ddl_probe'
```

Expect:

| n   |
| --- |
| 1   |
