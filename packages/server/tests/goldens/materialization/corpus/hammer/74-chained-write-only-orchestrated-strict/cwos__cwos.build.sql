-- hammer/74-chained-write-only-orchestrated-strict/cwos/cwos.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cwos/cwos.malloy
-- connection: orders_pg
-- sourceEntityId: 02e7093897c36710ae20395fe7a6f7b846582809951b7bc6930ddcbcbe77a6f5
-- compiler key: 02e7093897c36710ae20395fe7a6f7b846582809951b7bc6930ddcbcbe77a6f5 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cwos_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/cwos/cwos.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/cwos/cwos.malloy
-- sourceEntityId: a0ae1a2cb1d4498e1a0fb2816ccf8b8000cb5adc203de9249226cc69bcbbe3ba
-- compiler key: a0ae1a2cb1d4498e1a0fb2816ccf8b8000cb5adc203de9249226cc69bcbbe3ba (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.cwos_orders as base
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
