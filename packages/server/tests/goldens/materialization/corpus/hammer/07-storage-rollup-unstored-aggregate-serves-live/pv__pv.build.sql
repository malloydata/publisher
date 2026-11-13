-- hammer/07-storage-rollup-unstored-aggregate-serves-live/pv/pv.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__preagg__category_qty__1f32fd3a
-- sourceID: orders__preagg__category_qty__1f32fd3a@<hammer>/pv/pv.malloy.preagg.malloy
-- connection: orders_pg
-- sourceEntityId: cc21cfb7f21b6d88679631ae8c3816b77d0c4954749c44d0c9bc9f03522b99a6
-- compiler key: cc21cfb7f21b6d88679631ae8c3816b77d0c4954749c44d0c9bc9f03522b99a6 (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   base."qty" as "qty",
   (COALESCE(SUM(base."amount"),0)) as "total__partial"
FROM public.pv_orders as base
GROUP BY 1,2
ORDER BY 3 desc NULLS LAST

-- generated files

-- ==== <hammer>/pv/pv.malloy.preagg.malloy
##! experimental { persistence composite_sources }
import { orders__preagg_base is orders } from "pv.malloy"

#@ persist storage="lake"
source: orders__preagg__category_qty__1f32fd3a is orders__preagg_base -> {
  group_by:
    category
    qty
  aggregate:
    total__partial is total
} extend {
  measure:
    total is total__partial.sum()
}

source: orders is compose(orders__preagg_base)
