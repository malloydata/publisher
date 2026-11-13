-- conformance: query-shaped: two stages [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source two_stage
-- sourceID: two_stage@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 2c9a41b07b97b53c525b3357ea2ac18d040c22dc82548fe282a2d81b9ea3a549
-- compiler key: 2c9a41b07b97b53c525b3357ea2ac18d040c22dc82548fe282a2d81b9ea3a549 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."customer_id" as "customer_id",
     base."region" as "region",
     COALESCE(SUM(base."amount"),0) as "t"
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
)
, __stage1 AS (
  SELECT 
     base."region" as "region",
     COUNT(1) as "customers",
     COALESCE(SUM(base."t"),0) as "t2"
  FROM __stage0 as base
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   base."customers" as "customers",
   base."t2" as "t2"
FROM __stage1 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { two_stage__mat__base is two_stage } from 'm.malloy'

#@ persist name="two_stage" storage="lake"
source: two_stage__mat is two_stage__mat__base -> {
  select:
    region,
    customers,
    t2
}
