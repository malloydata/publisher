-- hammer/03-storage-rollup-mode-off/pm/pm.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__rollup__category__845156d3
-- sourceID: orders__rollup__category__845156d3@<hammer>/pm/pm.malloy.preaggregate.malloy
-- connection: orders_pg
-- sourceEntityId: ac71344c5b1efc51d294817ea1e5eaf04a97994793bf671038bbe0cf57cb6d32
-- compiler key: ac71344c5b1efc51d294817ea1e5eaf04a97994793bf671038bbe0cf57cb6d32 (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   COALESCE(SUM(base."amount"),0) as "total__partial"
FROM public.pm_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <hammer>/pm/pm.malloy.preaggregate.malloy
##! experimental.persistence
import { orders__rollup_base is orders } from "pm.malloy"

#@ persist storage="lake"
source: orders__rollup__category__845156d3 is orders__rollup_base -> {
  group_by:
    category
  aggregate:
    total__partial is amount.sum()
}
