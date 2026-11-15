-- hammer/05-partition-column-must-be-in-the-stored-table/pcs/pcs.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders
-- sourceID: orders@<hammer>/pcs/pcs.malloy
-- connection: orders_pg
-- sourceEntityId: f25005ccbf931398d0174c0dcd557882f39cf9b3eca35686ba8da6bd68c2190d
-- compiler key: f25005ccbf931398d0174c0dcd557882f39cf9b3eca35686ba8da6bd68c2190d (the sourceEntityId)
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."order_id" as "order_id",
   base."org_id" as "org_id"
FROM (SELECT order_id, org_id, amount FROM public.pcs_orders) as base
