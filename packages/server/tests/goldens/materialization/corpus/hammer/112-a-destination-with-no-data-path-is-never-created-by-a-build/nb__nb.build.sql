-- hammer/112-a-destination-with-no-data-path-is-never-created-by-a-build/nb/nb.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/nb/nb.malloy
-- connection: orders_pg
-- sourceEntityId: 2b0e91417e61b013be49e8d458eb94a09a98ac13ec1e40ece11de77ac690bb38
-- compiler key: 2b0e91417e61b013be49e8d458eb94a09a98ac13ec1e40ece11de77ac690bb38 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.nb_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
