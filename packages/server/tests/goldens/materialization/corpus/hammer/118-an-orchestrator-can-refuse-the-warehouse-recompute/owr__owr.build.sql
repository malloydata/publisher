-- hammer/118-an-orchestrator-can-refuse-the-warehouse-recompute/owr/owr.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/owr/owr.malloy
-- connection: orders_pg
-- sourceEntityId: 1997ed9b6f5e8e658f6671e2e85ab91fca60278f5f894dff7ae59251290160de
-- compiler key: 1997ed9b6f5e8e658f6671e2e85ab91fca60278f5f894dff7ae59251290160de (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.owr_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source daily_regional
-- sourceID: daily_regional@<hammer>/owr/owr.malloy
-- connection: orders_pg
-- sourceEntityId: 1997ed9b6f5e8e658f6671e2e85ab91fca60278f5f894dff7ae59251290160de
-- compiler key: 1997ed9b6f5e8e658f6671e2e85ab91fca60278f5f894dff7ae59251290160de (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.owr_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST
