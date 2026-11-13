-- conformance: same-named sources in two models, no name= [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source summary
-- sourceID: summary@<pkg>/a.malloy
-- connection: duckdb
-- sourceEntityId: 29824ca9cc0d97d7e28a17d8bf3586eb937dd7e2e8de3585a3f3415d42bb3fd3
-- compiler key: 29824ca9cc0d97d7e28a17d8bf3586eb937dd7e2e8de3585a3f3415d42bb3fd3 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
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
WHERE base."status"='placed'
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source summary
-- sourceID: summary@<pkg>/b.malloy
-- connection: duckdb
-- sourceEntityId: 16482181baf5fb53a84c99ab0dc9d66ece79526da60885db3ea36f7c9c0617b7
-- compiler key: 16482181baf5fb53a84c99ab0dc9d66ece79526da60885db3ea36f7c9c0617b7 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
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
WHERE base."status"='shipped'
GROUP BY 1
ORDER BY 2 desc NULLS LAST
