-- hammer/55-extended-source-inherits-persist/esi/esi.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/esi/esi.malloy
-- connection: orders_pg
-- sourceEntityId: 5699b6e1d50645661a4ae27c571761c92aa7af8e56a55e7eb11a13b3f012145b
-- compiler key: 5699b6e1d50645661a4ae27c571761c92aa7af8e56a55e7eb11a13b3f012145b (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "num_orders"
FROM public.esi_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source daily_with_avg
-- sourceID: daily_with_avg@<hammer>/esi/esi.malloy
-- connection: orders_pg
-- sourceEntityId: 5699b6e1d50645661a4ae27c571761c92aa7af8e56a55e7eb11a13b3f012145b
-- compiler key: 5699b6e1d50645661a4ae27c571761c92aa7af8e56a55e7eb11a13b3f012145b (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "num_orders"
FROM public.esi_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
