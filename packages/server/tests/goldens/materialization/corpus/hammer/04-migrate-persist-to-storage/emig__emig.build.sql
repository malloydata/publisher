-- hammer/04-migrate-persist-to-storage/emig/emig.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/emig/emig.malloy
-- connection: orders_pg
-- sourceEntityId: d844a20cd5cd3838f7274041b99b5fa868167af068c6d03c3a76ff0c23774ff1
-- compiler key: d844a20cd5cd3838f7274041b99b5fa868167af068c6d03c3a76ff0c23774ff1 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.emig_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
