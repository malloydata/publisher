-- hammer/96-an-intermediate-over-an-inline-extend-is-carried/iie/iie.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/iie/iie.malloy
-- connection: orders_pg
-- sourceEntityId: ac1cb5568b19bda1d9472ea4e97ec242e757ea623b8155df506cea8602f06df2
-- compiler key: ac1cb5568b19bda1d9472ea4e97ec242e757ea623b8155df506cea8602f06df2 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.iie_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source regions_kept
-- sourceID: regions_kept@<hammer>/iie/iie.malloy
-- connection: orders_pg
-- sourceEntityId: 040a61d03225555b2a8bc5216a1b0bbd245b84185b194929736b1eeb664fd11f
-- compiler key: 040a61d03225555b2a8bc5216a1b0bbd245b84185b194929736b1eeb664fd11f (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   base."region_id" as "region_id"
FROM public.iie_regions as base

-- ==== source rollup
-- sourceID: rollup@<hammer>/iie/iie.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/iie/iie.malloy, regions_kept@<hammer>/iie/iie.malloy
-- sourceEntityId: 33a0a8a71a03697f7e667ee6b75e9c0a964f7608fb06f7717431b5f1ce167150
-- compiler key: 33a0a8a71a03697f7e667ee6b75e9c0a964f7608fb06f7717431b5f1ce167150 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region_id" as "region_id",
     COALESCE(SUM(base."amount"),0) as "total"
  FROM public.iie_orders as base
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     base."region" as "region",
     base."region_id" as "region_id"
  FROM public.iie_regions as base
)
, __stage2 AS (
  SELECT 
     r_0."region" as "region",
     COALESCE(SUM(base."total"),0) as "total"
  FROM __stage0 as base
   LEFT JOIN __stage1 AS r_0
    ON base."region_id"=r_0."region_id"
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."total"),0) as "grand_total"
FROM __stage2 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
-- build SQL with every other planned source built:
WITH __stage0 AS (
  SELECT 
     r_0."region" as "region",
     COALESCE(SUM(base."total"),0) as "total"
  FROM built.daily as base
   LEFT JOIN built.regions_kept AS r_0
    ON base."region_id"=r_0."region_id"
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."total"),0) as "grand_total"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
