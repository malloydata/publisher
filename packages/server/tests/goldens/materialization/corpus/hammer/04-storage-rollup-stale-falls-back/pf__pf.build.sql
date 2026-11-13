-- hammer/04-storage-rollup-stale-falls-back/pf/pf.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__preagg__category__edb2cd3b
-- sourceID: orders__preagg__category__edb2cd3b@<hammer>/pf/pf.malloy.preagg.malloy
-- connection: orders_pg
-- sourceEntityId: 6e89839a2cf70ca3309e37e296a87709386edacc747051c84f4026b4527fb913
-- compiler key: 6e89839a2cf70ca3309e37e296a87709386edacc747051c84f4026b4527fb913 (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   (COALESCE(SUM(base."amount"),0)) as "total__partial"
FROM public.pf_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <hammer>/pf/pf.malloy.preagg.malloy
##! experimental { persistence composite_sources }
import { orders__preagg_base is orders } from "pf.malloy"

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
