-- hammer/22-skip-unchanged-and-force-refresh/sk/sk.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/sk/sk.malloy
-- connection: orders_pg
-- sourceEntityId: 7f47528f5b330add23b1b0e559e0011707575766a7e67fd9b384c23c4657831b
-- compiler key: 7f47528f5b330add23b1b0e559e0011707575766a7e67fd9b384c23c4657831b (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.sk_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
