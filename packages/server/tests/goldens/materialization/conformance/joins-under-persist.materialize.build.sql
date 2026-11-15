-- conformance: joins under persist [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source line_totals
-- sourceID: line_totals@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 31a18323a69306c9fbffb933cc967780fa76d52875d8acf21548991c45f31816
-- compiler key: 31a18323a69306c9fbffb933cc967780fa76d52875d8acf21548991c45f31816 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     COUNT(l_0."line_id") as "lines",
     AVG(CASE WHEN l_0."line_id" IS NOT NULL THEN l_0."qty" END) as "avg_qty",
     COUNT(DISTINCT base."id") as "n"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
   LEFT JOIN (
    SELECT CAST(line_id AS BIGINT) AS line_id, CAST(order_id AS BIGINT) AS order_id,
           sku, CAST(qty AS BIGINT) AS qty
    FROM read_csv('data/lines.csv', header=true, all_varchar=true)
  ) AS l_0
    ON l_0."order_id"=base."id"
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   base."lines" as "lines",
   base."avg_qty" as "avg_qty",
   base."n" as "n"
FROM __stage0 as base

-- ==== source segment_totals
-- sourceID: segment_totals@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: b23aac1dbb6d26baf32516b16ab8c3292dc0d6095ffb4bdb354ebf614eb8e206
-- compiler key: b23aac1dbb6d26baf32516b16ab8c3292dc0d6095ffb4bdb354ebf614eb8e206 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     c_0."segment" as "segment",
     COALESCE((
          SELECT SUM(a.val) as value
          FROM (
            SELECT UNNEST(list(distinct {key:base."id", val: base."amount"})) a
          )
        ),0) as "t"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
   LEFT JOIN (
    SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
           CAST(signup_date AS DATE) AS signup_date
    FROM read_csv('data/customers.csv', header=true, all_varchar=true)
  ) AS c_0
    ON c_0."id"=base."customer_id"
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."segment" as "segment",
   base."t" as "t"
FROM __stage0 as base

-- ==== source with_lines
-- sourceID: with_lines@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 4fdc1f71fde2e75f144819b490bd3b432a0253f77143d86000817fe0bcaeab0e
-- compiler key: 4fdc1f71fde2e75f144819b490bd3b432a0253f77143d86000817fe0bcaeab0e (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."id" as "id",
     base."region" as "region",
     base."amount" as "amount"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
)
SELECT 
   base."id" as "id",
   base."region" as "region",
   base."amount" as "amount"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { line_totals__mat__base is line_totals } from 'm.malloy'
import { segment_totals__mat__base is segment_totals } from 'm.malloy'
import { with_lines__mat__base is with_lines } from 'm.malloy'

#@ persist name="line_totals" storage="lake"
source: line_totals__mat is line_totals__mat__base -> {
  select:
    region,
    lines,
    avg_qty,
    n
}

#@ persist name="segment_totals" storage="lake"
source: segment_totals__mat is segment_totals__mat__base -> {
  select:
    segment,
    t
}

#@ persist name="with_lines" storage="lake"
source: with_lines__mat is with_lines__mat__base -> {
  select:
    id,
    region,
    amount
}
