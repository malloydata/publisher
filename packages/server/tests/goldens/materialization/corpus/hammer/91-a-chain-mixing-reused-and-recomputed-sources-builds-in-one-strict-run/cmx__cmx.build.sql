-- hammer/91-a-chain-mixing-reused-and-recomputed-sources-builds-in-one-strict-run/cmx/cmx.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cmx/cmx.malloy
-- connection: orders_pg
-- sourceEntityId: 6c78dad00d882fc718ce688bee839aa9a7f485814d626bdee75c57e674110350
-- compiler key: 6c78dad00d882fc718ce688bee839aa9a7f485814d626bdee75c57e674110350 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cmx_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source daily_regional
-- sourceID: daily_regional@<hammer>/cmx/cmx.malloy
-- connection: orders_pg
-- sourceEntityId: 6c78dad00d882fc718ce688bee839aa9a7f485814d626bdee75c57e674110350
-- compiler key: 6c78dad00d882fc718ce688bee839aa9a7f485814d626bdee75c57e674110350 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cmx_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source summary
-- sourceID: summary@<hammer>/cmx/cmx.malloy
-- connection: orders_pg
-- sourceEntityId: 75c6692d5e685ea56574888f62625f4c8405c38e9971b26940cbaeee891ce286
-- compiler key: 75c6692d5e685ea56574888f62625f4c8405c38e9971b26940cbaeee891ce286 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     base."region_id" as "region_id",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.cmx_orders as base
  GROUP BY 1,2
  ORDER BY 1 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     r_0."region" as "region",
     COALESCE(SUM(base."total_amount"),0) as "total"
  FROM __stage0 as base
   LEFT JOIN public.cmx_regions AS r_0
    ON base."region_id"=r_0."region_id"
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
, __stage2 AS (
  SELECT 
     base."region" as "region",
     base."total" as "total"
  FROM __stage1 as base
)
SELECT 
   COALESCE(SUM(base."total"),0) as "grand_total"
FROM __stage2 as base
-- build SQL with every other planned source built:
WITH __stage0 AS (
  SELECT 
     r_0."region" as "region",
     COALESCE(SUM(base."total_amount"),0) as "total"
  FROM built.daily_regional as base
   LEFT JOIN public.cmx_regions AS r_0
    ON base."region_id"=r_0."region_id"
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     base."region" as "region",
     base."total" as "total"
  FROM __stage0 as base
)
SELECT 
   COALESCE(SUM(base."total"),0) as "grand_total"
FROM __stage1 as base
