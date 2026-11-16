-- hammer/95-a-carried-intermediate-reading-a-given-still-compiles/cig/cig.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cig/cig.malloy
-- connection: orders_pg
-- sourceEntityId: e27cb5498cfa910170634ba0b5bbc714d81aedb8abb90539cae65d24835f62b0
-- compiler key: e27cb5498cfa910170634ba0b5bbc714d81aedb8abb90539cae65d24835f62b0 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.cig_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/cig/cig.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/cig/cig.malloy
-- sourceEntityId: 31679b00e56acf24b4de3827230b7b7847a807b8f7a444f69684202a4af92fc3
-- compiler key: 31679b00e56acf24b4de3827230b7b7847a807b8f7a444f69684202a4af92fc3 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     base."region" as "region",
     COALESCE(SUM(base."amount"),0) as "total"
  FROM public.cig_orders as base
  GROUP BY 1,2
  ORDER BY 1 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     base."order_date" as "order_date",
     base."region" as "region",
     base."total" as "total"
  FROM __stage0 as base
)
SELECT 
   COALESCE(SUM(base."total"),0) as "grand_total"
FROM __stage1 as base
-- build SQL with every other planned source built:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     base."region" as "region",
     base."total" as "total"
  FROM built.daily as base
)
SELECT 
   COALESCE(SUM(base."total"),0) as "grand_total"
FROM __stage0 as base
