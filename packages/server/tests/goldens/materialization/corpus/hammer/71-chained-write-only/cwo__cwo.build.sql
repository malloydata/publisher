-- hammer/71-chained-write-only/cwo/cwo.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cwo/cwo.malloy
-- connection: orders_pg
-- sourceEntityId: 04d5219ebfb9be9beb295a1ff05807f1dcd25b740d96e59ca649e36a5a67a4bd
-- compiler key: 04d5219ebfb9be9beb295a1ff05807f1dcd25b740d96e59ca649e36a5a67a4bd (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cwo_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/cwo/cwo.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/cwo/cwo.malloy
-- sourceEntityId: efe8fe51a7a252aab414b1c8765d50adf4f2352f84d93ee942ca70bb56355abc
-- compiler key: efe8fe51a7a252aab414b1c8765d50adf4f2352f84d93ee942ca70bb56355abc (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.cwo_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage0 as base
-- build SQL with every other planned source built:
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM built.daily as base
