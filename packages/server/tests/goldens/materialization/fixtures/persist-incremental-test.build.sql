-- fixture: persist-incremental-test: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<pkg>/persist_incremental_test.malloy
-- connection: duckdb
-- sourceEntityId: 7af77a0318aa2c4a95edbe2cc3e12bff6e01d87f880d902c73854596b97e233f
-- compiler key: 7af77a0318aa2c4a95edbe2cc3e12bff6e01d87f880d902c73854596b97e233f (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."category" as "category",
   COUNT(1) as "total_orders",
   COALESCE(SUM(base."amount"),0) as "total_revenue"
FROM 'data/orders.csv' as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST
