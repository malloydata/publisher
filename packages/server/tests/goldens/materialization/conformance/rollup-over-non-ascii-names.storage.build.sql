-- conformance: rollup over non-ASCII names [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source bestellungen__rollup______278821ac
-- sourceID: bestellungen__rollup______278821ac@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: d670287e216cd08c509b60bdc2522c5aca3f578822c264f6556c51d147200323
-- compiler key: d670287e216cd08c509b60bdc2522c5aca3f578822c264f6556c51d147200323 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "地域",
   COALESCE(SUM(base."amount"),0) as "summe__partial",
   COUNT(1) as "件数__partial"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy.preaggregate.malloy
##! experimental.persistence
import { bestellungen__rollup_base is bestellungen } from "m.malloy"

#@ persist storage="lake"
source: bestellungen__rollup______278821ac is bestellungen__rollup_base -> {
  group_by:
    `地域`
  aggregate:
    summe__partial is amount.sum()
    `件数__partial` is count()
}
