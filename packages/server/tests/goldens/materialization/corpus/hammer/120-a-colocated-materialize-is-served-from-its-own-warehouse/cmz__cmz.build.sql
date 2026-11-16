-- hammer/120-a-colocated-materialize-is-served-from-its-own-warehouse/cmz/cmz.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source big_orders
-- sourceID: big_orders@<hammer>/cmz/cmz.malloy
-- connection: orders_pg
-- sourceEntityId: 61bfdd2aa3526c6a1a6379b588dff6e28f0707b1802f70e133c77b4ad602a2d4
-- compiler key: 61bfdd2aa3526c6a1a6379b588dff6e28f0707b1802f70e133c77b4ad602a2d4 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."region" as "region",
   base."amount" as "amount",
   CASE WHEN (base."amount">=500) THEN 'large' ELSE 'medium' END as "tier"
FROM public.cmz_orders as base
WHERE base."amount">=100

-- generated files

-- ==== <hammer>/cmz/cmz.malloy.materialize.malloy
##! experimental.persistence
import { big_orders__mat__base is big_orders } from 'cmz.malloy'

#@ persist name="cmz_big"
source: big_orders__mat is big_orders__mat__base -> {
  select:
    order_id,
    region,
    amount,
    tier
}
