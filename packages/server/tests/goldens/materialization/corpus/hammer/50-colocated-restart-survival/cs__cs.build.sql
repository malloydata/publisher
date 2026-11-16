-- hammer/50-colocated-restart-survival/cs/cs.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cs/cs.malloy
-- connection: orders_pg
-- sourceEntityId: 13ac021bf3329e7ff66c610d268277c80237b282ffa80e41aa6209944d96ea66
-- compiler key: 13ac021bf3329e7ff66c610d268277c80237b282ffa80e41aa6209944d96ea66 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cs_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
