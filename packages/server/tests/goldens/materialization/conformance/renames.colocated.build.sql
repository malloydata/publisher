-- conformance: renames [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_r
-- sourceID: daily_r@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 610a61e5c5ec7535205405f43a14d7dc85aa4f8bde6e4f35f3cb2ba1073294e5
-- compiler key: 610a61e5c5ec7535205405f43a14d7dc85aa4f8bde6e4f35f3cb2ba1073294e5 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "area",
   base."order_date" as "order_date",
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
ORDER BY 2 desc NULLS LAST

-- ==== source daily_r2
-- sourceID: daily_r2@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 610a61e5c5ec7535205405f43a14d7dc85aa4f8bde6e4f35f3cb2ba1073294e5
-- compiler key: 610a61e5c5ec7535205405f43a14d7dc85aa4f8bde6e4f35f3cb2ba1073294e5 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "area",
   base."order_date" as "order_date",
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
ORDER BY 2 desc NULLS LAST

-- ==== source daily_r3
-- sourceID: daily_r3@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 610a61e5c5ec7535205405f43a14d7dc85aa4f8bde6e4f35f3cb2ba1073294e5
-- compiler key: 610a61e5c5ec7535205405f43a14d7dc85aa4f8bde6e4f35f3cb2ba1073294e5 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "area",
   base."order_date" as "order_date",
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
ORDER BY 2 desc NULLS LAST
