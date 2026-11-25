-- conformance: persisted view over a persisted source [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source pv_base
-- sourceID: pv_base@<pkg>/m.malloy
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

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { pv_base__mat__base is pv_base } from 'm.malloy'

#@ persist name="pv_base" storage="lake"
source: pv_base__mat is pv_base__mat__base -> {
  select:
    id,
    region,
    status,
    amount
}
