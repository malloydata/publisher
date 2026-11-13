-- hammer/01-a-delta-advances-a-colocated-table/iv/iv.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source batch_totals
-- sourceID: batch_totals@<hammer>/iv/iv.malloy
-- connection: orders_pg
-- sourceEntityId: d65ae9ee11b57b7d107f13ee2d927f72dc168a89d40988000f1c8bb68ea1a0bb
-- compiler key: d65ae9ee11b57b7d107f13ee2d927f72dc168a89d40988000f1c8bb68ea1a0bb (the sourceEntityId)
-- build SQL:
SELECT 
   base."batch" as "batch",
   COUNT(1) as "order_count",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.iv_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
