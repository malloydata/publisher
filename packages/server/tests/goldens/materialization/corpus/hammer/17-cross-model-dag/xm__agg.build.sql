-- hammer/17-cross-model-dag/xm/agg.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/xm/agg.malloy
-- connection: orders_pg
-- sourceEntityId: 63d8dd1dc6aeee48cfe006a886d4bae882b9bcca39c2e0bf7708366cecdc0a9f
-- compiler key: 63d8dd1dc6aeee48cfe006a886d4bae882b9bcca39c2e0bf7708366cecdc0a9f (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.xm_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
