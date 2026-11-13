-- conformance: extensions of persisted sources [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source base_daily
-- sourceID: base_daily@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 175f2f43939e80ff617d5364d7ab308c4eb085496735fa2e2a87fc98d9c331ad
-- compiler key: 175f2f43939e80ff617d5364d7ab308c4eb085496735fa2e2a87fc98d9c331ad (the sourceEntityId)
-- build SQL:
SELECT 
   base."customer_id" as "customer_id",
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
GROUP BY 1,2,3,4
ORDER BY 4 desc NULLS LAST

-- ==== source ext1
-- sourceID: ext1@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 175f2f43939e80ff617d5364d7ab308c4eb085496735fa2e2a87fc98d9c331ad
-- compiler key: 175f2f43939e80ff617d5364d7ab308c4eb085496735fa2e2a87fc98d9c331ad (the sourceEntityId)
-- build SQL:
SELECT 
   base."customer_id" as "customer_id",
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
GROUP BY 1,2,3,4
ORDER BY 4 desc NULLS LAST

-- ==== source ext2
-- sourceID: ext2@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 175f2f43939e80ff617d5364d7ab308c4eb085496735fa2e2a87fc98d9c331ad
-- compiler key: 175f2f43939e80ff617d5364d7ab308c4eb085496735fa2e2a87fc98d9c331ad (the sourceEntityId)
-- build SQL:
SELECT 
   base."customer_id" as "customer_id",
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
GROUP BY 1,2,3,4
ORDER BY 4 desc NULLS LAST

-- ==== source ext_joined
-- sourceID: ext_joined@<pkg>/joined.malloy
-- connection: duckdb
-- sourceEntityId: 175f2f43939e80ff617d5364d7ab308c4eb085496735fa2e2a87fc98d9c331ad
-- compiler key: 175f2f43939e80ff617d5364d7ab308c4eb085496735fa2e2a87fc98d9c331ad (the sourceEntityId)
-- build SQL:
SELECT 
   base."customer_id" as "customer_id",
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
GROUP BY 1,2,3,4
ORDER BY 4 desc NULLS LAST

-- ==== source ext_other
-- sourceID: ext_other@<pkg>/other.malloy
-- connection: duckdb
-- sourceEntityId: 175f2f43939e80ff617d5364d7ab308c4eb085496735fa2e2a87fc98d9c331ad
-- compiler key: 175f2f43939e80ff617d5364d7ab308c4eb085496735fa2e2a87fc98d9c331ad (the sourceEntityId)
-- build SQL:
SELECT 
   base."customer_id" as "customer_id",
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
GROUP BY 1,2,3,4
ORDER BY 4 desc NULLS LAST
