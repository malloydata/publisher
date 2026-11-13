-- hammer/101-a-chained-build-honours-the-parents-own-where/pow/pow.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/pow/pow.malloy
-- connection: orders_pg
-- sourceEntityId: da948460b5d6502931ca9f86fd869d810bbdf141b4a9533d3255071595e54840
-- compiler key: da948460b5d6502931ca9f86fd869d810bbdf141b4a9533d3255071595e54840 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.pow_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/pow/pow.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/pow/pow.malloy
-- sourceEntityId: ac73059f2d2e96c361028db929c2655c05cee2fb50faac3574446180a1367142
-- compiler key: ac73059f2d2e96c361028db929c2655c05cee2fb50faac3574446180a1367142 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.pow_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage0 as base
WHERE base."total_amount">100
-- build SQL with every other planned source built:
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM built.daily as base
WHERE base."total_amount">100

-- ==== source rollup_wide
-- sourceID: rollup_wide@<hammer>/pow/pow.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/pow/pow.malloy
-- sourceEntityId: e4a9b36d870cf7eae94fb53d317300f830933e333b93f795ba93fd34c8096ae4
-- compiler key: e4a9b36d870cf7eae94fb53d317300f830933e333b93f795ba93fd34c8096ae4 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.pow_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     base."order_date" as "order_date",
     base."total_amount" as "total_amount"
  FROM __stage0 as base
  WHERE base."total_amount">100
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage1 as base
-- build SQL with every other planned source built:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     base."total_amount" as "total_amount"
  FROM built.daily as base
  WHERE base."total_amount">100
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage0 as base
