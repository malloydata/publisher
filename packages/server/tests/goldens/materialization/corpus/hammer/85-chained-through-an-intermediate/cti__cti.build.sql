-- hammer/85-chained-through-an-intermediate/cti/cti.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cti/cti.malloy
-- connection: orders_pg
-- sourceEntityId: e65255304b213b45b04d9021f7bce7502de93d2f49b6f8ca33b364b6206fd293
-- compiler key: e65255304b213b45b04d9021f7bce7502de93d2f49b6f8ca33b364b6206fd293 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cti_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/cti/cti.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/cti/cti.malloy
-- sourceEntityId: 40f5f840284ecb473274db7715d9d756feb8ff569add70721df0c8653074be25
-- compiler key: 40f5f840284ecb473274db7715d9d756feb8ff569add70721df0c8653074be25 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.cti_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
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
     base."order_date" as "order_date",
     base."total_amount" as "total_amount"
  FROM built.daily as base
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage0 as base
