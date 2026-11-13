-- hammer/14-chained-visibility-leak-guard/chv/chv.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/chv/chv.malloy
-- connection: orders_pg
-- sourceEntityId: dd81ffda61591cfd3fdd71ca8b05a84ef34a4c12e0aff4fa37a29bfefc5097f0
-- compiler key: dd81ffda61591cfd3fdd71ca8b05a84ef34a4c12e0aff4fa37a29bfefc5097f0 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.chv_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/chv/chv.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/chv/chv.malloy
-- sourceEntityId: 916655bef3e5990a7592e3ee3b894aa63bafa69315263fd5dda140675acf7581
-- compiler key: 916655bef3e5990a7592e3ee3b894aa63bafa69315263fd5dda140675acf7581 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     base."region" as "region",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.chv_orders as base
  GROUP BY 1,2
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   base."order_date" as "order_date",
   base."region" as "region",
   COALESCE(SUM(base."total_amount"),0) as "t"
FROM __stage0 as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   base."order_date" as "order_date",
   base."region" as "region",
   COALESCE(SUM(base."total_amount"),0) as "t"
FROM built.daily as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST
