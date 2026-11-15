-- conformance: chained persisted sources [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders_base
-- sourceID: orders_base@<pkg>/base.malloy
-- connection: duckdb
-- sourceEntityId: 97cee6ed0a8888fb660e2e917f3c4f607e64b96bf1a7a16fb0fa46ef2a3c72ef
-- compiler key: 97cee6ed0a8888fb660e2e917f3c4f607e64b96bf1a7a16fb0fa46ef2a3c72ef (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   base."status" as "status",
   COALESCE(SUM(base."amount"),0) as "total_amount"
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

-- ==== source orders_rollup
-- sourceID: orders_rollup@<pkg>/base.malloy
-- connection: duckdb
-- reads: orders_base@<pkg>/base.malloy
-- sourceEntityId: b6534da2ba5d208bc5b68eb2c803c48c2b7b07e91c58e988059c3ddd45063463
-- compiler key: b6534da2ba5d208bc5b68eb2c803c48c2b7b07e91c58e988059c3ddd45063463 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     base."status" as "status",
     COALESCE(SUM(base."amount"),0) as "total_amount"
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
)
SELECT 
   base."status" as "status",
   COUNT(1) as "regions",
   COALESCE(SUM(base."total_amount"),0) as "t"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   base."status" as "status",
   COUNT(1) as "regions",
   COALESCE(SUM(base."total_amount"),0) as "t"
FROM built.orders_base as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source orders_rollup_x
-- sourceID: orders_rollup_x@<pkg>/rollup.malloy
-- connection: duckdb
-- reads: orders_base@<pkg>/base.malloy
-- sourceEntityId: 45a4da84fffc90542390bef0d62f1c943d873438f24950c85b63b050632755bd
-- compiler key: 45a4da84fffc90542390bef0d62f1c943d873438f24950c85b63b050632755bd (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     base."status" as "status",
     COALESCE(SUM(base."amount"),0) as "total_amount"
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
)
SELECT 
   base."region" as "region",
   COUNT(1) as "statuses",
   COALESCE(SUM(base."total_amount"),0) as "t"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   base."region" as "region",
   COUNT(1) as "statuses",
   COALESCE(SUM(base."total_amount"),0) as "t"
FROM built.orders_base as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
