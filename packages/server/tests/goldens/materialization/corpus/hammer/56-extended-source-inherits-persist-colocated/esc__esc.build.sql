-- hammer/56-extended-source-inherits-persist-colocated/esc/esc.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/esc/esc.malloy
-- connection: orders_pg
-- sourceEntityId: cca41dbf6d0f0b9aface3cd2d1995416d4867fcebae6acede78e1d2622658bc6
-- compiler key: cca41dbf6d0f0b9aface3cd2d1995416d4867fcebae6acede78e1d2622658bc6 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "num_orders"
FROM public.esc_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source daily_with_avg
-- sourceID: daily_with_avg@<hammer>/esc/esc.malloy
-- connection: orders_pg
-- sourceEntityId: cca41dbf6d0f0b9aface3cd2d1995416d4867fcebae6acede78e1d2622658bc6
-- compiler key: cca41dbf6d0f0b9aface3cd2d1995416d4867fcebae6acede78e1d2622658bc6 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "num_orders"
FROM public.esc_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
