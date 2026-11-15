-- hammer/02-partition-lays-the-artifact-out/pla/pla.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders
-- sourceID: orders@<hammer>/pla/pla.malloy
-- connection: orders_pg
-- sourceEntityId: 5b4b8f458a0daba28a1c7bd6c14e50cbbb78ae53232cdff8d309a400a4ab9a67
-- compiler key: 2dba74c60a5b45dae95c454cee72dc79452b923a0c23ba1d794aa8fb057a8a86
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."order_id" as "order_id",
   base."org_id" as "org_id"
FROM (SELECT order_id, org_id, amount FROM public.pla_orders) as base
