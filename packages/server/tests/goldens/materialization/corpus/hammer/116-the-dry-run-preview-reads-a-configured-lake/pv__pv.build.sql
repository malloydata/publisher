-- hammer/116-the-dry-run-preview-reads-a-configured-lake/pv/pv.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/pv/pv.malloy
-- connection: orders_pg
-- sourceEntityId: 0e625173fcccfd2ff786ece9516dd07d77cdf5c5b4ef6a67fef1c1eb77750fb7
-- compiler key: 0e625173fcccfd2ff786ece9516dd07d77cdf5c5b4ef6a67fef1c1eb77750fb7 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.pv_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST
