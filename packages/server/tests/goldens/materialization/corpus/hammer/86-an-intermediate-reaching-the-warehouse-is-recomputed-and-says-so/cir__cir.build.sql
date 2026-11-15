-- hammer/86-an-intermediate-reaching-the-warehouse-is-recomputed-and-says-so/cir/cir.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cir/cir.malloy
-- connection: orders_pg
-- sourceEntityId: 33320394f8188366ea042c3ac0534847d382e4269c7996535f2af388271ab36c
-- compiler key: 33320394f8188366ea042c3ac0534847d382e4269c7996535f2af388271ab36c (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cir_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source daily_regional
-- sourceID: daily_regional@<hammer>/cir/cir.malloy
-- connection: orders_pg
-- sourceEntityId: 33320394f8188366ea042c3ac0534847d382e4269c7996535f2af388271ab36c
-- compiler key: 33320394f8188366ea042c3ac0534847d382e4269c7996535f2af388271ab36c (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cir_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/cir/cir.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/cir/cir.malloy, daily_regional@<hammer>/cir/cir.malloy
-- sourceEntityId: 312f51bb638530683f77aa76d1c15d5fcdc1bc5c8edd43393d9a3615109c9831
-- compiler key: 312f51bb638530683f77aa76d1c15d5fcdc1bc5c8edd43393d9a3615109c9831 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     base."region_id" as "region_id",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.cir_orders as base
  GROUP BY 1,2
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   r_0."region" as "region",
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage0 as base
 LEFT JOIN public.cir_regions AS r_0
  ON base."region_id"=r_0."region_id"
GROUP BY 1
ORDER BY 2 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   r_0."region" as "region",
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM built.daily_regional as base
 LEFT JOIN public.cir_regions AS r_0
  ON base."region_id"=r_0."region_id"
GROUP BY 1
ORDER BY 2 desc NULLS LAST
