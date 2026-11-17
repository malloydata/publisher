-- feature: v1 preaggregate: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__rollup__m_region__75e7c0f9
-- sourceID: orders__rollup__m_region__75e7c0f9@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: 63ab64738419e7bc208520ea3386607248fa4a63e035a05092114b5fbc7085f7
-- compiler key: 63ab64738419e7bc208520ea3386607248fa4a63e035a05092114b5fbc7085f7 (the sourceEntityId)
-- build SQL:
SELECT 
   DATE_TRUNC('month', base."created_at") as "m",
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total__partial"
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
ORDER BY 1 desc NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy.preaggregate.malloy
##! experimental.persistence
import { orders__rollup_base is orders } from "m.malloy"

#@ persist
source: orders__rollup__m_region__75e7c0f9 is orders__rollup_base -> {
  group_by:
    m
    region
  aggregate:
    total__partial is amount.sum()
}
