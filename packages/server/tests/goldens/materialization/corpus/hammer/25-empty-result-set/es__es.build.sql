-- hammer/25-empty-result-set/es/es.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source big_orders
-- sourceID: big_orders@<hammer>/es/es.malloy
-- connection: orders_pg
-- sourceEntityId: 48be0a249f141f3ba8022ac68c80dc938a5f49947369199c21a9e27b015db1e0
-- compiler key: 48be0a249f141f3ba8022ac68c80dc938a5f49947369199c21a9e27b015db1e0 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.es_orders as base
WHERE base."amount">1000000
GROUP BY 1
ORDER BY 1 desc NULLS LAST
