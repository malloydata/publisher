-- fixture: persist-advisory-test: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<pkg>/persist_advisory_test.malloy
-- connection: duckdb
-- sourceEntityId: 3e2e0d29260223019303fa0c3cbb3679421f9c8a7dd2c9ff514138e9b30592bc
-- compiler key: 3e2e0d29260223019303fa0c3cbb3679421f9c8a7dd2c9ff514138e9b30592bc (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."category" as "category",
   COUNT(1) as "total_orders",
   COALESCE(SUM(base."amount"),0) as "total_revenue"
FROM (
  SELECT DATE '2024-05-01' AS order_date, 'electronics' AS category,
         100 AS amount, 'o-1' AS order_id
) as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST
