-- hammer/119-a-storage-extension-table-builds-over-its-parents-stored-table/mzx/mzx.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders
-- sourceID: orders@<hammer>/mzx/mzx.malloy
-- connection: orders_pg
-- sourceEntityId: d347e975ea5cb13142022738114dfa7bbd2cc74c58c3c6cb7ed0b32d7e126aa6
-- compiler key: d347e975ea5cb13142022738114dfa7bbd2cc74c58c3c6cb7ed0b32d7e126aa6 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."order_date" as "order_date",
   base."region" as "region",
   base."amount" as "amount",
   DATE_TRUNC('month', base."order_date") as "order_month"
FROM public.mzx_orders as base

-- ==== source us_orders
-- sourceID: us_orders@<hammer>/mzx/mzx.malloy
-- connection: orders_pg
-- reads: orders@<hammer>/mzx/mzx.malloy
-- sourceEntityId: 6b2fdcff71afff25ee88e5bdd43ece2d2ca744fb5ae4d7d9cacff61d19a73116
-- compiler key: 6b2fdcff71afff25ee88e5bdd43ece2d2ca744fb5ae4d7d9cacff61d19a73116 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_id" as "order_id",
     base."order_date" as "order_date",
     base."region" as "region",
     base."amount" as "amount",
     DATE_TRUNC('month', base."order_date") as "order_month"
  FROM public.mzx_orders as base
)
SELECT 
   base."order_id" as "order_id",
   base."order_date" as "order_date",
   base."region" as "region",
   base."amount" as "amount",
   base."order_month" as "order_month",
   base."amount">100 as "big"
FROM __stage0 as base
WHERE base."region"='US'
-- build SQL with every other planned source built:
SELECT 
   base."order_id" as "order_id",
   base."order_date" as "order_date",
   base."region" as "region",
   base."amount" as "amount",
   base."order_month" as "order_month",
   base."amount">100 as "big"
FROM built.orders as base
WHERE base."region"='US'

-- ==== source us_big
-- sourceID: us_big@<hammer>/mzx/mzx.malloy
-- connection: orders_pg
-- reads: us_orders@<hammer>/mzx/mzx.malloy
-- sourceEntityId: d3b92b1b9e027f20758f5316e9428dde4ffa99cfd32c57bdfd9b2eaf84e00fa2
-- compiler key: d3b92b1b9e027f20758f5316e9428dde4ffa99cfd32c57bdfd9b2eaf84e00fa2 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_id" as "order_id",
     base."order_date" as "order_date",
     base."region" as "region",
     base."amount" as "amount",
     DATE_TRUNC('month', base."order_date") as "order_month"
  FROM public.mzx_orders as base
)
, __stage1 AS (
  SELECT 
     base."order_id" as "order_id",
     base."order_date" as "order_date",
     base."region" as "region",
     base."amount" as "amount",
     base."order_month" as "order_month",
     base."amount">100 as "big"
  FROM __stage0 as base
  WHERE base."region"='US'
)
SELECT 
   base."order_id" as "order_id",
   base."order_date" as "order_date",
   base."region" as "region",
   base."amount" as "amount",
   base."order_month" as "order_month",
   base."big" as "big"
FROM __stage1 as base
WHERE (base."region"='US')
AND (base."big")
-- build SQL with every other planned source built:
SELECT 
   base."order_id" as "order_id",
   base."order_date" as "order_date",
   base."region" as "region",
   base."amount" as "amount",
   base."order_month" as "order_month",
   base."big" as "big"
FROM built.us_orders as base
WHERE (base."region"='US')
AND (base."big")

-- generated files

-- ==== <hammer>/mzx/mzx.malloy.0.materialize-build.malloy
import { orders__mat } from 'mzx.malloy.0.materialize-level.malloy'
source: orders_raw is orders_pg.table('public.mzx_orders')

#(materialize) storage="lake"
source: orders is orders__mat extend {
  measure: total is amount.sum()
}

#(materialize) storage="lake"
source: us_orders is orders extend {
  where: region = 'US'
  dimension: big is amount > 100
}

#(materialize) storage="lake"
source: us_big is us_orders extend {
  where: big
}

-- ==== <hammer>/mzx/mzx.malloy.0.materialize-level.malloy
##! experimental.persistence
import { orders__mat__base is orders } from 'mzx.malloy'

#@ persist name="orders" storage="lake"
source: orders__mat is orders__mat__base -> {
  select:
    order_id,
    order_date,
    region,
    amount,
    order_month
}

-- ==== <hammer>/mzx/mzx.malloy.1.materialize-build.malloy
import { us_orders__mat } from 'mzx.malloy.1.materialize-level.malloy'
source: orders_raw is orders_pg.table('public.mzx_orders')

#(materialize) storage="lake"
source: orders is orders_raw extend {
  dimension: order_month is order_date.month
  measure: total is amount.sum()
}

#(materialize) storage="lake"
source: us_orders is us_orders__mat extend {
  where: region = 'US'
}

#(materialize) storage="lake"
source: us_big is us_orders extend {
  where: big
}

-- ==== <hammer>/mzx/mzx.malloy.1.materialize-level.malloy
##! experimental.persistence
import { us_orders__mat__base is us_orders } from 'mzx.malloy.0.materialize-build.malloy'

#@ persist name="us_orders" storage="lake"
source: us_orders__mat is us_orders__mat__base -> {
  select:
    order_id,
    order_date,
    region,
    amount,
    order_month,
    big
}

-- ==== <hammer>/mzx/mzx.malloy.2.materialize-level.malloy
##! experimental.persistence
import { us_big__mat__base is us_big } from 'mzx.malloy.1.materialize-build.malloy'

#@ persist name="us_big" storage="lake"
source: us_big__mat is us_big__mat__base -> {
  select:
    order_id,
    order_date,
    region,
    amount,
    order_month,
    big
}

-- ==== <hammer>/mzx/mzx.malloy.materialize.malloy
##! experimental.persistence
import { orders__mat } from 'mzx.malloy.0.materialize-level.malloy'
import { us_orders__mat } from 'mzx.malloy.1.materialize-level.malloy'
import { us_big__mat } from 'mzx.malloy.2.materialize-level.malloy'
