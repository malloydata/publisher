-- hammer/102-a-persisted-extension-builds-its-own-table-under-strict/pex/pex.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/pex/pex.malloy
-- connection: orders_pg
-- sourceEntityId: ea3d999aa399c1f74cea22ac56a2d80a6cb73e11b188e6a63109f9e4dd41a97e
-- compiler key: ea3d999aa399c1f74cea22ac56a2d80a6cb73e11b188e6a63109f9e4dd41a97e (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "num_orders"
FROM public.pex_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source daily_wide
-- sourceID: daily_wide@<hammer>/pex/pex.malloy
-- connection: orders_pg
-- sourceEntityId: ea3d999aa399c1f74cea22ac56a2d80a6cb73e11b188e6a63109f9e4dd41a97e
-- compiler key: ea3d999aa399c1f74cea22ac56a2d80a6cb73e11b188e6a63109f9e4dd41a97e (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "num_orders"
FROM public.pex_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
