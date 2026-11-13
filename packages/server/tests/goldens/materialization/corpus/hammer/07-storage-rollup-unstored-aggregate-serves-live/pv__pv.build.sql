-- hammer/07-storage-rollup-unstored-aggregate-serves-live/pv/pv.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__rollup__category_qty__3eb75032
-- sourceID: orders__rollup__category_qty__3eb75032@<hammer>/pv/pv.malloy.preaggregate.malloy
-- connection: orders_pg
-- sourceEntityId: 068a7a95c90d97a93a56ecc6dd01c907558c76bc520e060c6f16e07b9893109b
-- compiler key: 068a7a95c90d97a93a56ecc6dd01c907558c76bc520e060c6f16e07b9893109b (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   base."qty" as "qty",
   COALESCE(SUM(base."amount"),0) as "total__partial"
FROM public.pv_orders as base
GROUP BY 1,2
ORDER BY 3 desc NULLS LAST

-- generated files

-- ==== <hammer>/pv/pv.malloy.preaggregate.malloy
##! experimental.persistence
import { orders__rollup_base is orders } from "pv.malloy"

#@ persist storage="lake"
source: orders__rollup__category_qty__3eb75032 is orders__rollup_base -> {
  group_by:
    category
    qty
  aggregate:
    total__partial is amount.sum()
}
