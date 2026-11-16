-- conformance: imports and re-exports [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<pkg>/facts.malloy
-- connection: duckdb
-- sourceEntityId: 59d4ddddd6f5bab96fa706a9cfdc282e29caaf772da4c47b93b896997392a632
-- compiler key: 59d4ddddd6f5bab96fa706a9cfdc282e29caaf772da4c47b93b896997392a632 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   base."status" as "status",
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
GROUP BY 1,2,3
ORDER BY 3 desc NULLS LAST

-- ==== source d_plus
-- sourceID: d_plus@<pkg>/dash.malloy
-- connection: duckdb
-- sourceEntityId: 59d4ddddd6f5bab96fa706a9cfdc282e29caaf772da4c47b93b896997392a632
-- compiler key: 59d4ddddd6f5bab96fa706a9cfdc282e29caaf772da4c47b93b896997392a632 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   base."status" as "status",
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
GROUP BY 1,2,3
ORDER BY 3 desc NULLS LAST

-- ==== source daily_ext
-- sourceID: daily_ext@<pkg>/facts.malloy
-- connection: duckdb
-- sourceEntityId: 59d4ddddd6f5bab96fa706a9cfdc282e29caaf772da4c47b93b896997392a632
-- compiler key: 59d4ddddd6f5bab96fa706a9cfdc282e29caaf772da4c47b93b896997392a632 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   base."status" as "status",
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
GROUP BY 1,2,3
ORDER BY 3 desc NULLS LAST
