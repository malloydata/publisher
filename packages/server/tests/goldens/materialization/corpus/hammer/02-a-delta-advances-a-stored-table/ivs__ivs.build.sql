-- hammer/02-a-delta-advances-a-stored-table/ivs/ivs.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source batch_totals
-- sourceID: batch_totals@<hammer>/ivs/ivs.malloy
-- connection: orders_pg
-- sourceEntityId: 1287b4c97b06ab8af57b36671ba5414c604f67ba0bf8487fd57bf45d902da858
-- compiler key: 1287b4c97b06ab8af57b36671ba5414c604f67ba0bf8487fd57bf45d902da858 (the sourceEntityId)
-- build SQL:
SELECT 
   base."batch" as "batch",
   COUNT(1) as "order_count",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.ivs_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
