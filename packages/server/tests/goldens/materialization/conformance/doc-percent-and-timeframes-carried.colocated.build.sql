-- conformance: doc, percent and timeframes carried [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source monthly
-- sourceID: monthly@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 513e827245f13473e04bf62e6047e330cedd1e27f39aa4cb3b1c245a62de04dc
-- compiler key: 513e827245f13473e04bf62e6047e330cedd1e27f39aa4cb3b1c245a62de04dc (the sourceEntityId)
-- build SQL:
SELECT 
   DATE_TRUNC('month', base."order_date") as "order_month",
   DATE_TRUNC('hour', base."created_at") as "created_hour",
   (DATE_TRUNC('week', (base."order_date" + INTERVAL '1' DAY)) - INTERVAL '1' DAY) as "order_week",
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total",
   AVG(base."discount") as "discount_rate"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1,2,3,4
ORDER BY 1 desc NULLS LAST
