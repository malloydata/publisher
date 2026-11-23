-- hammer/91-a-chain-mixing-reused-and-recomputed-sources-builds-in-one-strict-run/cmx/cmx.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cmx/cmx.malloy
-- connection: orders_pg
-- sourceEntityId: 6c78dad00d882fc718ce688bee839aa9a7f485814d626bdee75c57e674110350
-- compiler key: 6c78dad00d882fc718ce688bee839aa9a7f485814d626bdee75c57e674110350 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cmx_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source daily_regional
-- sourceID: daily_regional@<hammer>/cmx/cmx.malloy
-- connection: orders_pg
-- sourceEntityId: 6c78dad00d882fc718ce688bee839aa9a7f485814d626bdee75c57e674110350
-- compiler key: 6c78dad00d882fc718ce688bee839aa9a7f485814d626bdee75c57e674110350 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cmx_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST
