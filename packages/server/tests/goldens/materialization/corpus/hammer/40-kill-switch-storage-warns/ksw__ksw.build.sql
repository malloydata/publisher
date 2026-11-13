-- hammer/40-kill-switch-storage-warns/ksw/ksw.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/ksw/ksw.malloy
-- connection: orders_pg
-- sourceEntityId: 4f4fa1e20a21addecf420af842c990c17ad95c8c1af7956dd18a633e9ac8f27b
-- compiler key: 4f4fa1e20a21addecf420af842c990c17ad95c8c1af7956dd18a633e9ac8f27b (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.ksw_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
