-- conformance: imports and re-exports [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<pkg>/facts.malloy
-- connection: duckdb
-- sourceEntityId: a8999c602090e6871ad5e0137fc4a5219414160eaee4711736ac1e1a84f96cd9
-- compiler key: a8999c602090e6871ad5e0137fc4a5219414160eaee4711736ac1e1a84f96cd9 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     base."status" as "status",
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total",
     COUNT(1) as "n"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
  GROUP BY 1,2,3
  ORDER BY 3 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   base."status" as "status",
   base."order_date" as "order_date",
   base."total" as "total",
   base."n" as "n"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/facts.malloy.materialize.malloy
##! experimental.persistence
import { daily__mat__base is daily } from 'facts.malloy'

#@ persist name="daily" storage="lake"
source: daily__mat is daily__mat__base -> {
  select:
    region,
    status,
    order_date,
    total,
    n
}
