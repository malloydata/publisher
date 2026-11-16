-- conformance: inline extend in joins [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source by_customer
-- sourceID: by_customer@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 6bc7af71a54db5d246910da579ced5edf7bf42b4a14071f11bdff2212cc0a973
-- compiler key: 6bc7af71a54db5d246910da579ced5edf7bf42b4a14071f11bdff2212cc0a973 (the sourceEntityId)
-- build SQL:
SELECT 
   base."customer_id" as "customer_id",
   UPPER(c_0."segment") as "seg_up",
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
 LEFT JOIN (
  SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
         CAST(signup_date AS DATE) AS signup_date
  FROM read_csv('data/customers.csv', header=true, all_varchar=true)
) AS c_0
  ON base."customer_id"=c_0."id"
GROUP BY 1,2
ORDER BY 3 desc NULLS LAST

-- ==== source by_customer_plus
-- sourceID: by_customer_plus@<pkg>/plus.malloy
-- connection: duckdb
-- sourceEntityId: 6bc7af71a54db5d246910da579ced5edf7bf42b4a14071f11bdff2212cc0a973
-- compiler key: 6bc7af71a54db5d246910da579ced5edf7bf42b4a14071f11bdff2212cc0a973 (the sourceEntityId)
-- build SQL:
SELECT 
   base."customer_id" as "customer_id",
   UPPER(c_0."segment") as "seg_up",
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
 LEFT JOIN (
  SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
         CAST(signup_date AS DATE) AS signup_date
  FROM read_csv('data/customers.csv', header=true, all_varchar=true)
) AS c_0
  ON base."customer_id"=c_0."id"
GROUP BY 1,2
ORDER BY 3 desc NULLS LAST
