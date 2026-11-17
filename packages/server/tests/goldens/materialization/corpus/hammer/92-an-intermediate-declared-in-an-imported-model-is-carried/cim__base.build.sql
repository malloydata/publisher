-- hammer/92-an-intermediate-declared-in-an-imported-model-is-carried/cim/base.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cim/base.malloy
-- connection: orders_pg
-- sourceEntityId: cf7d087d64620ff1eb2c523463c0b6628858e8050579b6f504efd253c86afeb8
-- compiler key: cf7d087d64620ff1eb2c523463c0b6628858e8050579b6f504efd253c86afeb8 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "order_count"
FROM public.cim_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
