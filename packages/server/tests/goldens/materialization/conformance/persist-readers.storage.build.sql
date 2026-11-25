-- conformance: #@ -persist readers [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source summary
-- sourceID: summary@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 2375b815017e854f15706b749efb4d263a5bc0f59e2a56b56a5e4b2ccedd16da
-- compiler key: 2375b815017e854f15706b749efb4d263a5bc0f59e2a56b56a5e4b2ccedd16da (the sourceEntityId)
-- build SQL:
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

-- ==== source summary_kept
-- sourceID: summary_kept@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 2375b815017e854f15706b749efb4d263a5bc0f59e2a56b56a5e4b2ccedd16da
-- compiler key: 2375b815017e854f15706b749efb4d263a5bc0f59e2a56b56a5e4b2ccedd16da (the sourceEntityId)
-- build SQL:
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
