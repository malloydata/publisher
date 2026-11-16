-- hammer/43-authorize-refused/az/az.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/az/az.malloy
-- connection: orders_pg
-- sourceEntityId: 9442be6ad28254a8935b20635504036a68343102de66f6114514f04a44f0d18d
-- compiler key: 9442be6ad28254a8935b20635504036a68343102de66f6114514f04a44f0d18d (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.az_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
