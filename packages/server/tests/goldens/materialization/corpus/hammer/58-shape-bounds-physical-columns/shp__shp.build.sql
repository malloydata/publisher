-- hammer/58-shape-bounds-physical-columns/shp/shp.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/shp/shp.malloy
-- connection: orders_pg
-- sourceEntityId: 1d5d1b76f87e33f65cd19c385a02e96944053667d9bd7fd796612ef8a6b504f8
-- compiler key: 1d5d1b76f87e33f65cd19c385a02e96944053667d9bd7fd796612ef8a6b504f8 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.shp_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
