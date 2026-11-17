-- hammer/44-reclaim-drop-tables/gc/gc.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/gc/gc.malloy
-- connection: orders_pg
-- sourceEntityId: fb5cb32da5bc3c3d2d9355bd8bc378dae53f248ed8479e3068d7b560bfd10cf2
-- compiler key: fb5cb32da5bc3c3d2d9355bd8bc378dae53f248ed8479e3068d7b560bfd10cf2 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.gc_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
