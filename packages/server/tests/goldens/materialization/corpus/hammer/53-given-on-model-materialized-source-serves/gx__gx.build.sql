-- hammer/53-given-on-model-materialized-source-serves/gx/gx.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/gx/gx.malloy
-- connection: orders_pg
-- sourceEntityId: d4066df29187f2e7cb015cc002a7917bfa54f5026445ad4c0c643a4fcc0d2f9c
-- compiler key: d4066df29187f2e7cb015cc002a7917bfa54f5026445ad4c0c643a4fcc0d2f9c (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.gx_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
