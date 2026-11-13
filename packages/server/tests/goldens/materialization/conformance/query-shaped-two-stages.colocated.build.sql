-- conformance: query-shaped: two stages [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source two_stage
-- sourceID: two_stage@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 921510abab0f740ae898b326a5a3b03a271691c3a156ffd02ece7916f74c78f0
-- compiler key: 921510abab0f740ae898b326a5a3b03a271691c3a156ffd02ece7916f74c78f0 (the sourceEntityId)
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
SELECT 
   base."region" as "region",
   COUNT(1) as "customers",
   COALESCE(SUM(base."t"),0) as "t2"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
