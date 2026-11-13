-- hammer/77-extend-routes-to-the-base-table/erb/erb.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/erb/erb.malloy
-- connection: orders_pg
-- sourceEntityId: 4879af40cfeb86fb6982ff4b648b86b327244fbd980ca03f4ec5e4ea788251f1
-- compiler key: 4879af40cfeb86fb6982ff4b648b86b327244fbd980ca03f4ec5e4ea788251f1 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "num_orders"
FROM public.erb_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source daily_with_avg
-- sourceID: daily_with_avg@<hammer>/erb/erb.malloy
-- connection: orders_pg
-- sourceEntityId: 4879af40cfeb86fb6982ff4b648b86b327244fbd980ca03f4ec5e4ea788251f1
-- compiler key: 4879af40cfeb86fb6982ff4b648b86b327244fbd980ca03f4ec5e4ea788251f1 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "num_orders"
FROM public.erb_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
