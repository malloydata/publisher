-- hammer/07-schema-qualified-name/sq/sq.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/sq/sq.malloy
-- connection: orders_pg
-- sourceEntityId: ee38ea760948e2eda046481fb6588cb827e8aedc3f2325079cae2338c0c807a0
-- compiler key: ee38ea760948e2eda046481fb6588cb827e8aedc3f2325079cae2338c0c807a0 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.sq_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
