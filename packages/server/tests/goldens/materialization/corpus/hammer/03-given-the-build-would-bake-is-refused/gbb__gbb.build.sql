-- hammer/03-given-the-build-would-bake-is-refused/gbb/gbb.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders
-- sourceID: orders@<hammer>/gbb/gbb.malloy
-- connection: orders_pg
-- sourceEntityId: d485f048303fdb733e0d41da0347d8764809c3d7ba8c02fbc8e42948e3e77086
-- compiler key: d485f048303fdb733e0d41da0347d8764809c3d7ba8c02fbc8e42948e3e77086 (the sourceEntityId)
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."order_id" as "order_id",
   base."org_id" as "org_id"
FROM (SELECT order_id, org_id, amount FROM public.gbb_orders) as base
WHERE base."org_id"=1
