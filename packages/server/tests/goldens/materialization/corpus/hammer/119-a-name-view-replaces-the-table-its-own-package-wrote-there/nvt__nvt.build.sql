-- hammer/119-a-name-view-replaces-the-table-its-own-package-wrote-there/nvt/nvt.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/nvt/nvt.malloy
-- connection: orders_pg
-- sourceEntityId: 8bd0cc6964fd5e4945612e1bb0b75d4c154cbc3742504b806d548b466f2cfda6
-- compiler key: 8bd0cc6964fd5e4945612e1bb0b75d4c154cbc3742504b806d548b466f2cfda6 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.nvt_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   base."order_date" as "order_date",
   base."total_amount" as "total_amount"
FROM __stage0 as base

-- generated files

-- ==== <hammer>/nvt/nvt.malloy.materialize.malloy
##! experimental.persistence
import { daily_orders__mat__base is daily_orders } from 'nvt.malloy'

#@ persist name="nvt_daily" storage="lake"
source: daily_orders__mat is daily_orders__mat__base -> {
  select:
    order_date,
    total_amount
}
