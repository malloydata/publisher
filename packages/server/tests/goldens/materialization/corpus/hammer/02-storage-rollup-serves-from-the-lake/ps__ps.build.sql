-- hammer/02-storage-rollup-serves-from-the-lake/ps/ps.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__rollup__category__dd9e4c1c
-- sourceID: orders__rollup__category__dd9e4c1c@<hammer>/ps/ps.malloy.preaggregate.malloy
-- connection: orders_pg
-- sourceEntityId: e41e495afa2573d878d28ac199a392ebc6799aff2003d0a5401dc46194418ad3
-- compiler key: e41e495afa2573d878d28ac199a392ebc6799aff2003d0a5401dc46194418ad3 (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   COALESCE(SUM(base."amount"),0) as "total__partial"
FROM public.ps_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <hammer>/ps/ps.malloy.preaggregate.malloy
##! experimental.persistence
import { orders__rollup_base is orders } from "ps.malloy"

#@ persist storage="lake"
source: orders__rollup__category__dd9e4c1c is orders__rollup_base -> {
  group_by:
    category
  aggregate:
    total__partial is amount.sum()
}
