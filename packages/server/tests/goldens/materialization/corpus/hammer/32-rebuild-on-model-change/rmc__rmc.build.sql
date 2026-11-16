-- hammer/32-rebuild-on-model-change/rmc/rmc.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/rmc/rmc.malloy
-- connection: orders_pg
-- sourceEntityId: 722f756bd9cf5de205e4b9a4471bb23b9c071d1a95a7bc6f4bb92916fac5a95e
-- compiler key: 722f756bd9cf5de205e4b9a4471bb23b9c071d1a95a7bc6f4bb92916fac5a95e (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.rmc_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
