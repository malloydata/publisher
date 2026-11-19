-- hammer/04-storage-rollup-stale-falls-back/pf/pf.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__rollup__category__17b7053b
-- sourceID: orders__rollup__category__17b7053b@<hammer>/pf/pf.malloy.preaggregate.malloy
-- connection: orders_pg
-- sourceEntityId: f950521b89c57ada2bc2659cb944f87cd0bfc564e0df9b81bc5879e8df9a8e0e
-- compiler key: f950521b89c57ada2bc2659cb944f87cd0bfc564e0df9b81bc5879e8df9a8e0e (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   COALESCE(SUM(base."amount"),0) as "total__partial"
FROM public.pf_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <hammer>/pf/pf.malloy.preaggregate.malloy
##! experimental.persistence
import { orders__rollup_base is orders } from "pf.malloy"

#@ persist storage="lake"
source: orders__rollup__category__17b7053b is orders__rollup_base -> {
  group_by:
    category
  aggregate:
    total__partial is amount.sum()
}
