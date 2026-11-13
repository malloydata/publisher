-- hammer/01-serves-from-the-rollup/pa/pa.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

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

-- ==== source orders__preagg__category__edb2cd3b
-- sourceID: orders__preagg__category__edb2cd3b@<hammer>/pa/pa.malloy.preagg.malloy
-- connection: orders_pg
-- reads: orders@<hammer>/pa/pa.malloy
-- sourceEntityId: ced321f35ab4aed5db8b1c990a557726ac86a367ebc76878590f7b87634ff7b4
-- compiler key: ced321f35ab4aed5db8b1c990a557726ac86a367ebc76878590f7b87634ff7b4 (the sourceEntityId)
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
   (COALESCE(SUM(base."amount"),0)) as "total__partial"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   base."category" as "category",
   (COALESCE(SUM(base."amount"),0)) as "total__partial"
FROM built.orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source orders__preagg__region__c697d298
-- sourceID: orders__preagg__region__c697d298@<hammer>/pa/pa.malloy.preagg.malloy
-- connection: orders_pg
-- reads: orders@<hammer>/pa/pa.malloy
-- sourceEntityId: 4170edc90f093b5c545e588043f0a3034c5577f13fb2d32f3a7722058d39985b
-- compiler key: 4170edc90f093b5c545e588043f0a3034c5577f13fb2d32f3a7722058d39985b (the sourceEntityId)
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
   (COALESCE(SUM(base."amount"),0)) as "regional__partial"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   base."region" as "region",
   (COALESCE(SUM(base."amount"),0)) as "regional__partial"
FROM built.orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <hammer>/pa/pa.malloy.preagg.malloy
##! experimental { persistence composite_sources }
import { orders__preagg_base is orders } from "pa.malloy"

#@ persist name="analytics.orders__preagg__category__edb2cd3b"
source: orders__preagg__category__edb2cd3b is orders__preagg_base -> {
  group_by:
    category
  aggregate:
    total__partial is total
} extend {
  measure:
    total is total__partial.sum()
}

#@ persist name="rollups.orders__preagg__region__c697d298"
source: orders__preagg__region__c697d298 is orders__preagg_base -> {
  group_by:
    region
  aggregate:
    regional__partial is regional
} extend {
  measure:
    regional is regional__partial.sum()
}

source: orders is compose(orders__preagg__category__edb2cd3b, orders__preagg__region__c697d298, orders__preagg_base)
