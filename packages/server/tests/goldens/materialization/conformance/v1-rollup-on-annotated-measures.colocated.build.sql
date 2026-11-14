-- conformance: v1 rollup on annotated measures [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__rollup__region__409e9efb
-- sourceID: orders__rollup__region__409e9efb@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: f59e28dc263a95a93f57a37ddad239cafd219f8863304c4da1c005bc52e8ec18
-- compiler key: f59e28dc263a95a93f57a37ddad239cafd219f8863304c4da1c005bc52e8ec18 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COUNT(1) as "n__partial",
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
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy.preaggregate.malloy
##! experimental.persistence
import { orders__rollup_base is orders } from "m.malloy"

#@ persist
source: orders__rollup__region__409e9efb is orders__rollup_base -> {
  group_by:
    region
  aggregate:
    n__partial is count()
    total__partial is amount.sum()
}
