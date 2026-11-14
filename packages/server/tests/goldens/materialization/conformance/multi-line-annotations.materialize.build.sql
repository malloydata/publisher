-- conformance: multi-line annotations [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source annotated
-- sourceID: annotated@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: f1dee6fcac5854ef5cbc18ffe4c360fd800e9d68696ddab1c2c15deef9e85584
-- compiler key: f1dee6fcac5854ef5cbc18ffe4c360fd800e9d68696ddab1c2c15deef9e85584 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     DATE_TRUNC('month', base."order_date") as "order_month",
     COALESCE(SUM(base."amount"),0) as "total"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
  GROUP BY 1,2
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   base."order_month" as "order_month",
   base."total" as "total"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { annotated__mat__base is annotated } from 'm.malloy'

#@ persist name="annotated" storage="lake"
source: annotated__mat is annotated__mat__base -> {
  select:
    region,
    order_month,
    total
}
