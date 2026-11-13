-- hammer/45-serve-shape-drops-view/fbv/fbv.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders
-- sourceID: orders@<hammer>/fbv/fbv.malloy
-- connection: orders_pg
-- sourceEntityId: e56ae37301a4cdeba2b555dc80ffe3d6947ea81c94bd68198025b16ac9b8347a
-- compiler key: e56ae37301a4cdeba2b555dc80ffe3d6947ea81c94bd68198025b16ac9b8347a (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."amount" as "amount",
   base."region_id" as "region_id"
FROM public.fbv_orders as base
GROUP BY 1,2,3
ORDER BY 1 asc NULLS LAST
