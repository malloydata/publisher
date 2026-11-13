-- hammer/75-instruction-order-does-not-change-the-build/iob/iob.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/iob/iob.malloy
-- connection: orders_pg
-- sourceEntityId: 0416b96383e1693279bed5c6e6571eb4dc9d61869c7d0d21d4b3de0f37952c3c
-- compiler key: 0416b96383e1693279bed5c6e6571eb4dc9d61869c7d0d21d4b3de0f37952c3c (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.iob_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/iob/iob.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/iob/iob.malloy
-- sourceEntityId: f2fb2227eff44a9017d6606a3f354fd44ea476f4aba3bc952a9cf144acaa30cc
-- compiler key: f2fb2227eff44a9017d6606a3f354fd44ea476f4aba3bc952a9cf144acaa30cc (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.iob_orders as base
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
