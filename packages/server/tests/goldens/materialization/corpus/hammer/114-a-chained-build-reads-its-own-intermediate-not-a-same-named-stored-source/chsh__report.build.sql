-- hammer/114-a-chained-build-reads-its-own-intermediate-not-a-same-named-stored-source/chsh/report.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/chsh/report.malloy
-- connection: orders_pg
-- sourceEntityId: 14cd99945cf6383c589b20d1da469f2fb6dd934a468e023a6c8e0e821cbfc97e
-- compiler key: 14cd99945cf6383c589b20d1da469f2fb6dd934a468e023a6c8e0e821cbfc97e (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.chsh_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/chsh/report.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/chsh/report.malloy
-- sourceEntityId: baeb6cb10acd9d2b5f362b3a0c0bd0288018046d5aa7b6becf25ba279a848f1c
-- compiler key: baeb6cb10acd9d2b5f362b3a0c0bd0288018046d5aa7b6becf25ba279a848f1c (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.chsh_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."total_amount"),0) as "v"
  FROM __stage0 as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   COALESCE(SUM(base."v"),0) as "grand_total"
FROM __stage1 as base
-- build SQL with every other planned source built:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."total_amount"),0) as "v"
  FROM built.daily as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   COALESCE(SUM(base."v"),0) as "grand_total"
FROM __stage0 as base
