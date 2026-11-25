-- hammer/01-tenant-scoped-source-serves-per-caller/tsp/tsp.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders
-- sourceID: orders@<hammer>/tsp/tsp.malloy
-- connection: orders_pg
-- sourceEntityId: b70997a611486d9e251b1e3bf78bf04e973b4429213e5933e3a99c2cd7c2ab56
-- compiler key: b70997a611486d9e251b1e3bf78bf04e973b4429213e5933e3a99c2cd7c2ab56 (the sourceEntityId)
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."order_id" as "order_id",
   base."org_id" as "org_id"
FROM (SELECT order_id, org_id, amount FROM public.tsp_orders) as base
