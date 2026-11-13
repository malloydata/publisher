-- hammer/89-a-carried-intermediate-compiles-under-its-files-flags/cfl/cfl.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cfl/cfl.malloy
-- connection: orders_pg
-- sourceEntityId: 2ea78588915bc00b4efb8b94868a14ed503d2ea5d2ed0dedcd37042ca14f5ba5
-- compiler key: 2ea78588915bc00b4efb8b94868a14ed503d2ea5d2ed0dedcd37042ca14f5ba5 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "order_count"
FROM public.cfl_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/cfl/cfl.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/cfl/cfl.malloy
-- sourceEntityId: 198f637e0b29e9e18bec81e49117c69565ea81f82c50615107a9f02920d90db2
-- compiler key: 198f637e0b29e9e18bec81e49117c69565ea81f82c50615107a9f02920d90db2 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount",
     COUNT(1) as "order_count"
  FROM public.cfl_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     base."order_count" as "order_count",
     base."order_date" as "order_date",
     base."total_amount" as "total_amount"
  FROM __stage0 as base
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage1 as base
-- build SQL with every other planned source built:
WITH __stage0 AS (
  SELECT 
     base."order_count" as "order_count",
     base."order_date" as "order_date",
     base."total_amount" as "total_amount"
  FROM built.daily as base
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage0 as base
