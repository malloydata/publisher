-- hammer/03-storage-rollup-mode-off/pm/pm.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__preagg__category__edb2cd3b
-- sourceID: orders__preagg__category__edb2cd3b@<hammer>/pm/pm.malloy.preagg.malloy
-- connection: orders_pg
-- sourceEntityId: a7639bbdb7a98be09caf4cf9f3708fddc9a9ba4651efac9d9a800296ba462b08
-- compiler key: a7639bbdb7a98be09caf4cf9f3708fddc9a9ba4651efac9d9a800296ba462b08 (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   (COALESCE(SUM(base."amount"),0)) as "total__partial"
FROM public.pm_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <hammer>/pm/pm.malloy.preagg.malloy
##! experimental { persistence composite_sources }
import { orders__preagg_base is orders } from "pm.malloy"

#@ persist storage="lake"
source: orders__preagg__category__edb2cd3b is orders__preagg_base -> {
  group_by:
    category
  aggregate:
    total__partial is total
} extend {
  measure:
    total is total__partial.sum()
}

source: orders is compose(orders__preagg_base)
