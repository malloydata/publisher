-- conformance: a name with a space [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source spaced
-- sourceID: spaced@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 5af4cda2cb1755ea8666a6d3d3846c41477f2dbc3de37ef4b5618385021eb460
-- compiler key: 5af4cda2cb1755ea8666a6d3d3846c41477f2dbc3de37ef4b5618385021eb460 (the sourceEntityId)
-- build SQL:
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
