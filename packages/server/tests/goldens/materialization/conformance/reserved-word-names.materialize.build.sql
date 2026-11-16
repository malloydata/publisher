-- conformance: reserved-word names [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source order
-- sourceID: order@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: f4c625d477b98206d172cedd4c593f384037d09cbe230405a47b44a43fa84627
-- compiler key: f4c625d477b98206d172cedd4c593f384037d09cbe230405a47b44a43fa84627 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order" as "order",
     base."status" as "select",
     COUNT(1) as "count",
     COALESCE(SUM(base."amount"),0) as "amt"
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
  ORDER BY 3 desc NULLS LAST
)
SELECT 
   base."order" as "order",
   base."select" as "select",
   base."count" as "count",
   base."amt" as "amt"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { order__mat__base is order } from 'm.malloy'

#@ persist name="order" storage="lake"
source: order__mat is order__mat__base -> {
  select:
    order,
    `select`,
    `count`,
    amt
}
