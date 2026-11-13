-- feature: v1 preaggregate: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__preagg__m_region__3b5ca627
-- sourceID: orders__preagg__m_region__3b5ca627@<pkg>/m.malloy.preagg.malloy
-- connection: duckdb
-- sourceEntityId: cf4eac5212df2662deebb78b554263af3b76acc31bb380777d02b9023100c381
-- compiler key: cf4eac5212df2662deebb78b554263af3b76acc31bb380777d02b9023100c381 (the sourceEntityId)
-- build SQL:
SELECT 
   DATE_TRUNC('month', base."created_at") as "m",
   base."region" as "region",
   (COALESCE(SUM(base."amount"),0)) as "total__partial"
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

-- ==== <pkg>/m.malloy.preagg.malloy
##! experimental { persistence composite_sources }
import { orders__preagg_base is orders } from "m.malloy"

#@ persist
source: orders__preagg__m_region__3b5ca627 is orders__preagg_base -> {
  group_by:
    m
    region
  aggregate:
    total__partial is total
} extend {
  measure:
    total is total__partial.sum()
}

source: orders is compose(orders__preagg__m_region__3b5ca627, orders__preagg_base)
