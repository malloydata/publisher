-- hammer/02-refinements/d1/d1.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/d1/d1.malloy
-- connection: orders_pg
-- sourceEntityId: 1c18e73bc37a3997f1734e7b211b2c2361a79eab0258860864b5de33c55d2f3d
-- compiler key: 1c18e73bc37a3997f1734e7b211b2c2361a79eab0258860864b5de33c55d2f3d (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "order_count",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.d1_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
