-- conformance: query-shaped: from a view [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source from_view
-- sourceID: from_view@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 9ed55dbb1432fac7ffd6246709050d36f20372cdc3def07717591f4f42623980
-- compiler key: 9ed55dbb1432fac7ffd6246709050d36f20372cdc3def07717591f4f42623980 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     base."status" as "status",
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
  GROUP BY 1,2
  ORDER BY 3 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   base."status" as "status",
   base."total" as "total",
   base."n" as "n"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { from_view__mat__base is from_view } from 'm.malloy'

#@ persist name="from_view" storage="lake"
source: from_view__mat is from_view__mat__base -> {
  select:
    region,
    status,
    total,
    n
}
