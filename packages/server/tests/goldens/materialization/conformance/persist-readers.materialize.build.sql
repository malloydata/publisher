-- conformance: #@ -persist readers [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source summary
-- sourceID: summary@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 1f62ee077661338e4aa69c6e0c67fa4a23d17f33d7806206cd5caf829b2ff57e
-- compiler key: 1f62ee077661338e4aa69c6e0c67fa4a23d17f33d7806206cd5caf829b2ff57e (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     base."status" as "status",
     COALESCE(SUM(base."amount"),0) as "total"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
  GROUP BY 1,2
  ORDER BY 3 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   base."status" as "status",
   base."total" as "total"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { summary__mat__base is summary } from 'm.malloy'

#@ persist name="summary" storage="lake"
source: summary__mat is summary__mat__base -> {
  select:
    region,
    status,
    total
}
