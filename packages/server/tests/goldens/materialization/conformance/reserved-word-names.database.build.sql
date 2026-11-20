-- conformance: reserved-word names [database]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source order
-- sourceID: order@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: ddeab0253bd9657798b023c29fabb35badff349b5ffe3f3fd2e14f144ba0e635
-- compiler key: ddeab0253bd9657798b023c29fabb35badff349b5ffe3f3fd2e14f144ba0e635 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order" as "order",
   base."status" as "select",
   COUNT(1) as "count",
   COALESCE(SUM(base."amount"),0) as "amt"
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

-- ==== source month
-- sourceID: month@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: b1c24636d6f3b6c816d73f62633897d22a6daddc3b0713245fb2a83c0861cb48
-- compiler key: b1c24636d6f3b6c816d73f62633897d22a6daddc3b0713245fb2a83c0861cb48 (the sourceEntityId)
-- build SQL:
SELECT 
   EXTRACT(month FROM base."order_date") as "month",
   base."region" as "group",
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
