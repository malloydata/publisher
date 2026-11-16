-- hammer/01-serves-from-the-rollup/pa/pa.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__rollup__category__389984bb
-- sourceID: orders__rollup__category__389984bb@<hammer>/pa/pa.malloy.preaggregate.malloy
-- connection: orders_pg
-- sourceEntityId: 04803cb53503036fdb1f5e6b1b64ca6941a7f419563f2b6f31e222f7825a5bdc
-- compiler key: 04803cb53503036fdb1f5e6b1b64ca6941a7f419563f2b6f31e222f7825a5bdc (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_id" as "order_id",
     base."category" as "category",
     base."region" as "region",
     base."amount" as "amount"
  FROM public.pa_orders as base
)
SELECT 
   base."category" as "category",
   COALESCE(SUM(base."amount"),0) as "total__partial"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source orders__rollup__region__85a0fa84
-- sourceID: orders__rollup__region__85a0fa84@<hammer>/pa/pa.malloy.preaggregate.malloy
-- connection: orders_pg
-- sourceEntityId: 4f696de650cac5319e7ccbb2bf531fd7ba3c436d45ac7cea6a9ff42c8dce83f3
-- compiler key: 4f696de650cac5319e7ccbb2bf531fd7ba3c436d45ac7cea6a9ff42c8dce83f3 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_id" as "order_id",
     base."category" as "category",
     base."region" as "region",
     base."amount" as "amount"
  FROM public.pa_orders as base
)
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "regional__partial"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source orders
-- sourceID: orders@<hammer>/pa/pa.malloy
-- connection: orders_pg
-- sourceEntityId: 92d61a9306e44a690f5cf72f43d6fc00acf3c3f2d0c9298a70d7cdd211b05067
-- compiler key: 92d61a9306e44a690f5cf72f43d6fc00acf3c3f2d0c9298a70d7cdd211b05067 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."category" as "category",
   base."region" as "region",
   base."amount" as "amount"
FROM public.pa_orders as base

-- generated files

-- ==== <hammer>/pa/pa.malloy
##! experimental { persistence composite_sources }

source: pa_orders is orders_pg.table('public.pa_orders')

                                         
source: orders is pa_orders -> {
  select: order_id, category, region, amount
} extend {
  measure:
    #@ preaggregate grain="region" namespace="rollups"
    regional is amount.sum()
  measure:
    #@ preaggregate grain="category"
    total is amount.sum()
}

-- ==== <hammer>/pa/pa.malloy.preaggregate.malloy
##! experimental.persistence
import { orders__rollup_base is orders } from "pa.malloy"

#@ persist name="analytics.orders__rollup__category__389984bb"
source: orders__rollup__category__389984bb is orders__rollup_base -> {
  group_by:
    category
  aggregate:
    total__partial is amount.sum()
}

#@ persist name="rollups.orders__rollup__region__85a0fa84"
source: orders__rollup__region__85a0fa84 is orders__rollup_base -> {
  group_by:
    region
  aggregate:
    regional__partial is amount.sum()
}
