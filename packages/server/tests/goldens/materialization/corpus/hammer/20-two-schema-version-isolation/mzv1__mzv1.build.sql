-- hammer/20-two-schema-version-isolation/mzv1/mzv1.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/mzv1/mzv1.malloy
-- connection: orders_pg
-- sourceEntityId: 51f17ba2ff3031131d30c71b890c87232bc2374842805774a2c08415a71655e7
-- compiler key: 51f17ba2ff3031131d30c71b890c87232bc2374842805774a2c08415a71655e7 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.mz_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
