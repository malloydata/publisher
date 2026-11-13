-- hammer/28-bad-manifest-location/bml/bml.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/bml/bml.malloy
-- connection: orders_pg
-- sourceEntityId: 6d1c2c89cfb3d5726c7ac619f1bb8cb0eab5b8ce55aac08e2c43ef26ac4b3b7c
-- compiler key: 6d1c2c89cfb3d5726c7ac619f1bb8cb0eab5b8ce55aac08e2c43ef26ac4b3b7c (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.bml_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
