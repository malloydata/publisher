-- hammer/04-moving-a-source-to-storage-rebuilds-it/ivx/ivx.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source batch_totals
-- sourceID: batch_totals@<hammer>/ivx/ivx.malloy
-- connection: orders_pg
-- sourceEntityId: e75a78c5fffa495f44acadeae0f4920b0814df2e3db3c71c8d84b3777e22323c
-- compiler key: e75a78c5fffa495f44acadeae0f4920b0814df2e3db3c71c8d84b3777e22323c (the sourceEntityId)
-- build SQL:
SELECT 
   base."batch" as "batch",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.ivx_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
