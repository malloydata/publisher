-- hammer/109-an-orchestrator-drops-a-superseded-generation-and-reclaims-its-storage/dgc/dgc.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/dgc/dgc.malloy
-- connection: orders_pg
-- sourceEntityId: b3d675ff01d7acca706630e66c6ccd212379b80254c28a639f56b8c1642624c4
-- compiler key: b3d675ff01d7acca706630e66c6ccd212379b80254c28a639f56b8c1642624c4 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.dgc_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
