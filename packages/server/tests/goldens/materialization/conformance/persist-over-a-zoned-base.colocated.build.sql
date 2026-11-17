-- conformance: persist over a zoned base [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source inherits_zone
-- sourceID: inherits_zone@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 2ea58d1747888cfe4c616813579a380b00f03d1a4bbfd28942e6665ee41bc306
-- compiler key: 2ea58d1747888cfe4c616813579a380b00f03d1a4bbfd28942e6665ee41bc306 (the sourceEntityId)
-- build SQL:
SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)

-- ==== source sets_zone
-- sourceID: sets_zone@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 3cbd1059d9a03ac598fb3304141ff1896a1ff3cac14141df8d7ae75f46294a3a
-- compiler key: 3cbd1059d9a03ac598fb3304141ff1896a1ff3cac14141df8d7ae75f46294a3a (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."created_at" as "created_at",
   EXTRACT(hour FROM (base."created_at")::TIMESTAMPTZ AT TIME ZONE 'America/New_York') as "h",
   ((DATE_TRUNC('day', (base."created_at")::TIMESTAMPTZ AT TIME ZONE 'America/New_York')) AT TIME ZONE 'America/New_York')::TIMESTAMP as "d"
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
ORDER BY 2 desc NULLS LAST
