-- hammer/92-an-intermediate-declared-in-an-imported-model-is-carried/cim/agg.malloy: build plan
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

-- ==== source rollup
-- sourceID: rollup@<hammer>/cim/agg.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/cim/base.malloy
-- sourceEntityId: a3a214c891e0ab845be62a8fa88a472b7d57a157c972a21b11ac1e7664d7949e
-- compiler key: a3a214c891e0ab845be62a8fa88a472b7d57a157c972a21b11ac1e7664d7949e (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount",
     COUNT(1) as "order_count"
  FROM public.cim_orders as base
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
