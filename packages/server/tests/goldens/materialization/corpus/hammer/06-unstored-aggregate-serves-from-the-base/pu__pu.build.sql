-- hammer/06-unstored-aggregate-serves-from-the-base/pu/pu.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__rollup__category_qty__f372ae9d
-- sourceID: orders__rollup__category_qty__f372ae9d@<hammer>/pu/pu.malloy.preaggregate.malloy
-- connection: orders_pg
-- sourceEntityId: 1617c9e023a20383d7cdf30ed898350020ea2562b0b5e17b9ce4035d6e9af9f9
-- compiler key: 1617c9e023a20383d7cdf30ed898350020ea2562b0b5e17b9ce4035d6e9af9f9 (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   base."qty" as "qty",
   COALESCE(SUM(base."amount"),0) as "total__partial"
FROM public.pu_orders as base
GROUP BY 1,2
ORDER BY 3 desc NULLS LAST

-- generated files

-- ==== <hammer>/pu/pu.malloy.preaggregate.malloy
##! experimental.persistence
import { orders__rollup_base is orders } from "pu.malloy"

#@ persist
source: orders__rollup__category_qty__f372ae9d is orders__rollup_base -> {
  group_by:
    category
    qty
  aggregate:
    total__partial is amount.sum()
}
