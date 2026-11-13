-- hammer/24-multi-publisher-shared-table/cl/cl.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cl/cl.malloy
-- connection: orders_pg
-- sourceEntityId: f1d8f5f22fa50623f4a34b74bdc4935608e47f23df624f0134b1e140da6a4723
-- compiler key: f1d8f5f22fa50623f4a34b74bdc4935608e47f23df624f0134b1e140da6a4723 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cl_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
