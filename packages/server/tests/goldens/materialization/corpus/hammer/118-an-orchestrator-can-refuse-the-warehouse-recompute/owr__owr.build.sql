-- hammer/118-an-orchestrator-can-refuse-the-warehouse-recompute/owr/owr.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/owr/owr.malloy
-- connection: orders_pg
-- sourceEntityId: 1997ed9b6f5e8e658f6671e2e85ab91fca60278f5f894dff7ae59251290160de
-- compiler key: 1997ed9b6f5e8e658f6671e2e85ab91fca60278f5f894dff7ae59251290160de (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.owr_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source daily_regional
-- sourceID: daily_regional@<hammer>/owr/owr.malloy
-- connection: orders_pg
-- sourceEntityId: 1997ed9b6f5e8e658f6671e2e85ab91fca60278f5f894dff7ae59251290160de
-- compiler key: 1997ed9b6f5e8e658f6671e2e85ab91fca60278f5f894dff7ae59251290160de (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.owr_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/owr/owr.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/owr/owr.malloy, daily_regional@<hammer>/owr/owr.malloy
-- sourceEntityId: e9c5961d50654eb6b8849888003b5ad3e0ebf92d67be62e0d649608277f4c4b8
-- compiler key: e9c5961d50654eb6b8849888003b5ad3e0ebf92d67be62e0d649608277f4c4b8 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     base."region_id" as "region_id",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.owr_orders as base
  GROUP BY 1,2
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   r_0."region" as "region",
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage0 as base
 LEFT JOIN public.owr_regions AS r_0
  ON base."region_id"=r_0."region_id"
GROUP BY 1
ORDER BY 2 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   r_0."region" as "region",
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM built.daily_regional as base
 LEFT JOIN public.owr_regions AS r_0
  ON base."region_id"=r_0."region_id"
GROUP BY 1
ORDER BY 2 desc NULLS LAST
