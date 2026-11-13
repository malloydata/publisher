-- conformance: baseline [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: b5f87ec0aa79359dd819028840930acbc84c500ea8364a54bb7f54c323188829
-- compiler key: b5f87ec0aa79359dd819028840930acbc84c500ea8364a54bb7f54c323188829 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
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
GROUP BY 1,2
ORDER BY 2 desc NULLS LAST

-- ==== source daily_ext
-- sourceID: daily_ext@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: b5f87ec0aa79359dd819028840930acbc84c500ea8364a54bb7f54c323188829
-- compiler key: b5f87ec0aa79359dd819028840930acbc84c500ea8364a54bb7f54c323188829 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
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
GROUP BY 1,2
ORDER BY 2 desc NULLS LAST
