-- hammer/03-mode-matrix/b1/b1.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/b1/b1.malloy
-- connection: orders_pg
-- sourceEntityId: 48b6786652ed1a2c5bb96e040069c3c6d3b0292e01c84f74b08c3a1856b2cff5
-- compiler key: 48b6786652ed1a2c5bb96e040069c3c6d3b0292e01c84f74b08c3a1856b2cff5 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.b1_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
