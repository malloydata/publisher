-- hammer/02-storage-rollup-serves-from-the-lake/ps/ps.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__preagg__category__edb2cd3b
-- sourceID: orders__preagg__category__edb2cd3b@<hammer>/ps/ps.malloy.preagg.malloy
-- connection: orders_pg
-- sourceEntityId: d3e554bd1f5e0832ea5cebf2c224917d60347a4237bf81218fdd38b9d846b24e
-- compiler key: d3e554bd1f5e0832ea5cebf2c224917d60347a4237bf81218fdd38b9d846b24e (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   (COALESCE(SUM(base."amount"),0)) as "total__partial"
FROM public.ps_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <hammer>/ps/ps.malloy.preagg.malloy
##! experimental { persistence composite_sources }
import { orders__preagg_base is orders } from "ps.malloy"

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
