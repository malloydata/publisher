-- conformance: a name with a space [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source spaced
-- sourceID: spaced@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: ab7413d03101ccc433b90013f48de0df48c21d5097024c169a5d3b0d9b722f44
-- compiler key: ab7413d03101ccc433b90013f48de0df48c21d5097024c169a5d3b0d9b722f44 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."status" as "status",
     COALESCE(SUM(base."amount"),0) as "line total"
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
)
SELECT 
   base."status" as "status",
   base."line total" as "line total"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { spaced__mat__base is spaced } from 'm.malloy'

#@ persist name="spaced" storage="lake"
source: spaced__mat is spaced__mat__base -> {
  select:
    status,
    `line total`
}
