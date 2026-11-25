-- hammer/117-a-materialized-source-shows-only-its-authors-name/mzn/mzn.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source mzn_orders
-- sourceID: mzn_orders@<hammer>/mzn/mzn.malloy
-- connection: orders_pg
-- sourceEntityId: 62a03346515f7c1bd0c7749571ae4aac5e2144815737b41cac465a4459e428ec
-- compiler key: 62a03346515f7c1bd0c7749571ae4aac5e2144815737b41cac465a4459e428ec (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."order_date" as "order_date",
   base."region" as "region",
   base."amount" as "amount",
   DATE_TRUNC('month', base."order_date") as "order_month"
FROM public.mzn_orders as base

-- generated files

-- ==== <hammer>/mzn/mzn.malloy.materialize.malloy
##! experimental.persistence
import { mzn_orders__mat__base is mzn_orders } from 'mzn.malloy'

#@ persist name="mzn_orders_t" storage="lake"
source: mzn_orders__mat is mzn_orders__mat__base -> {
  select:
    order_id,
    order_date,
    region,
    amount,
    order_month
}
