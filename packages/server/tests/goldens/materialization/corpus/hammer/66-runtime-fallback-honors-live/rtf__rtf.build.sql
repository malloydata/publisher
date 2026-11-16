-- hammer/66-runtime-fallback-honors-live/rtf/rtf.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/rtf/rtf.malloy
-- connection: orders_pg
-- sourceEntityId: e80f56f9a7669807c26cc75604e1677b39291c396675d4f439ee242c4dff00e2
-- compiler key: e80f56f9a7669807c26cc75604e1677b39291c396675d4f439ee242c4dff00e2 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.rtf_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
