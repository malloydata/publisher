-- feature: given terms: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source own
-- sourceID: own@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: a8734965f12f8dbc5ae5774d10e6be2534b32640f161be6d1f2435f15f7cc21e
-- compiler key: a8734965f12f8dbc5ae5774d10e6be2534b32640f161be6d1f2435f15f7cc21e (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."customer_id" as "customer_id",
   base."region" as "region",
   base."status" as "status",
   base."amount" as "amount",
   base."discount" as "discount",
   base."order_date" as "order_date",
   base."created_at" as "created_at",
   base."order" as "order"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
WHERE true

-- generated files

-- ==== <pkg>/m.malloy.0.materialize-build.malloy
##! experimental { givens access_modifiers }
given:
  REGION :: string is 'north'
  MIN_AMT :: number is 0

import "raw.malloy"

// Its own given-reading where:.
#(materialize) storage="lake"
source: own is orders_raw extend {
  where: true
  measure:
    n is count()
    total is amount.sum()
}

source: base is orders_raw extend {
  where: region = $REGION
  dimension: at_least is amount >= $MIN_AMT
}

// A where: it inherits from its base.
#(materialize) storage="lake"
source: inherited is base extend {
  measure:
    n is count()
    total is amount.sum()
}

// A where: on a base's given-reading dimension.
#(materialize) storage="lake"
source: by_base_dimension is base extend {
  where: at_least
  measure: n is count()
}

// Both: an inherited term and one of its own.
#(materialize) storage="lake"
source: both is base extend {
  where: amount >= $MIN_AMT
  measure: n is count()
}

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental { persistence givens }
import { own__mat__base is own } from 'm.malloy.0.materialize-build.malloy'
import { REGION } from 'm.malloy'

#@ persist name="own" storage="lake"
source: own__mat is own__mat__base -> {
  select:
    id,
    customer_id,
    region,
    status,
    amount,
    discount,
    order_date,
    created_at,
    order
} extend {
  where: region = $REGION
}
