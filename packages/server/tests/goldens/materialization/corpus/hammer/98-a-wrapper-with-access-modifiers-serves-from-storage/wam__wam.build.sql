-- hammer/98-a-wrapper-with-access-modifiers-serves-from-storage/wam/wam.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/wam/wam.malloy
-- connection: orders_pg
-- sourceEntityId: cc710004d299e3cb91b211b5a8888419505331d88ca56fc90b113f755f97daa3
-- compiler key: cc710004d299e3cb91b211b5a8888419505331d88ca56fc90b113f755f97daa3 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "order_count"
FROM public.wam_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
