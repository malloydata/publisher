-- hammer/11-chained-persist/ch/ch.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/ch/ch.malloy
-- connection: orders_pg
-- sourceEntityId: 58863199ed171b3b4f87f3e4a8b314835b40ae3b3e767e684359abb7c91321d9
-- compiler key: 58863199ed171b3b4f87f3e4a8b314835b40ae3b3e767e684359abb7c91321d9 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.ch_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/ch/ch.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/ch/ch.malloy
-- sourceEntityId: a6f1bad4b1a4839527f57b80ea34f6b764d9a3d108df4a0c963f38e8a08d7c46
-- compiler key: a6f1bad4b1a4839527f57b80ea34f6b764d9a3d108df4a0c963f38e8a08d7c46 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.ch_orders as base
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
