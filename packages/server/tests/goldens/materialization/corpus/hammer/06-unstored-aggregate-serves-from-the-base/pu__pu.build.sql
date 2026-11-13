-- hammer/06-unstored-aggregate-serves-from-the-base/pu/pu.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__preagg__category_qty__1f32fd3a
-- sourceID: orders__preagg__category_qty__1f32fd3a@<hammer>/pu/pu.malloy.preagg.malloy
-- connection: orders_pg
-- sourceEntityId: 22d8864c721a97231777c765e7f25669befd0ef418028401a88139b8d167b502
-- compiler key: 22d8864c721a97231777c765e7f25669befd0ef418028401a88139b8d167b502 (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   base."qty" as "qty",
   (COALESCE(SUM(base."amount"),0)) as "total__partial"
FROM public.pu_orders as base
GROUP BY 1,2
ORDER BY 3 desc NULLS LAST

-- generated files

-- ==== <hammer>/pu/pu.malloy.preagg.malloy
##! experimental { persistence composite_sources }
import { orders__preagg_base is orders } from "pu.malloy"

#@ persist
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

source: orders is compose(orders__preagg__category_qty__1f32fd3a, orders__preagg_base)
