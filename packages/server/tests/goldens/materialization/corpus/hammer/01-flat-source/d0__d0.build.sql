-- hammer/01-flat-source/d0/d0.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/d0/d0.malloy
-- connection: orders_pg
-- sourceEntityId: c552f5e7e18d0c5cbf5f28fb0dee35f5aa77f96314aabebbc809f8be92f0f4a5
-- compiler key: c552f5e7e18d0c5cbf5f28fb0dee35f5aa77f96314aabebbc809f8be92f0f4a5 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "order_count",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
