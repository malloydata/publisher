-- conformance: givens and access filters [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source gated
-- sourceID: gated@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: cca1df00b9373e067e5ea94d46dc1396e1ca1d6841cf3b625670efbda19b4da2
-- compiler key: cca1df00b9373e067e5ea94d46dc1396e1ca1d6841cf3b625670efbda19b4da2 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."id" as "id",
     base."region" as "region",
     base."status" as "status",
     base."amount" as "amount"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
)
SELECT 
   base."id" as "id",
   base."region" as "region",
   base."status" as "status",
   base."amount" as "amount"
FROM __stage0 as base

-- ==== source scoped
-- sourceID: scoped@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 2b69286cf457bf6f154b27617edc214250de12c556ee4d57f1384435ad553a5e
-- compiler key: 2b69286cf457bf6f154b27617edc214250de12c556ee4d57f1384435ad553a5e (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."id" as "id",
     base."region" as "region",
     base."status" as "status",
     base."amount" as "amount",
     base."order_date" as "order_date"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
)
SELECT 
   base."id" as "id",
   base."region" as "region",
   base."status" as "status",
   base."amount" as "amount",
   base."order_date" as "order_date"
FROM __stage0 as base
WHERE true

-- generated files

-- ==== <pkg>/m.malloy.0.materialize-build.malloy
##! experimental { persistence givens }
import "raw.malloy"

given: REGION :: string is 'north'
given: REGIONS :: string[]

#(materialize) storage="lake"
source: scoped is orders_raw -> {
  select: id, region, status, amount, order_date
} extend {
  where: true
  measure: total is amount.sum()
}

#(access_filter) region in $REGIONS
source: gated_ext is scoped extend {}

#(access_filter) region in $REGIONS
#(materialize) storage="lake"
source: gated is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
}

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental { persistence givens }
import { gated__mat__base is gated } from 'm.malloy'
import { scoped__mat__base is scoped } from 'm.malloy.0.materialize-build.malloy'
import { REGION } from 'm.malloy'

#@ persist name="gated" storage="lake"
source: gated__mat is gated__mat__base -> {
  select:
    id,
    region,
    status,
    amount
}

#@ persist name="scoped" storage="lake"
source: scoped__mat is scoped__mat__base -> {
  select:
    id,
    region,
    status,
    amount,
    order_date
} extend {
  where: region = $REGION
}
