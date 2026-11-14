-- hammer/110-a-destination-nothing-has-been-built-into-lists-as-not-found/fresh/fresh.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/fresh/fresh.malloy
-- connection: orders_pg
-- sourceEntityId: 7ddffeb154b709b538f400c012d9ce131831bd6d3743b48fe83f2e95b5463d08
-- compiler key: 7ddffeb154b709b538f400c012d9ce131831bd6d3743b48fe83f2e95b5463d08 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.fresh_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
