-- conformance: colocated and storage= in one package [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source dim_c
-- sourceID: dim_c@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 820d6af004bafcff7762f225f8837cdf222b2d7dfa966299a5f66b074f48d280
-- compiler key: 820d6af004bafcff7762f225f8837cdf222b2d7dfa966299a5f66b074f48d280 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."segment" as "segment"
FROM (
  SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
         CAST(signup_date AS DATE) AS signup_date
  FROM read_csv('data/customers.csv', header=true, all_varchar=true)
) as base

-- ==== source fact_s
-- sourceID: fact_s@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: f68477c5c4c6c8217554876309cf1fe3d9adc67f71f024f9e5ccfd74d6ad1bee
-- compiler key: f68477c5c4c6c8217554876309cf1fe3d9adc67f71f024f9e5ccfd74d6ad1bee (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."customer_id" as "customer_id",
   base."region" as "region",
   base."amount" as "amount"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base

-- ==== source fact_joined
-- sourceID: fact_joined@<pkg>/joined.malloy
-- connection: duckdb
-- sourceEntityId: f68477c5c4c6c8217554876309cf1fe3d9adc67f71f024f9e5ccfd74d6ad1bee
-- compiler key: f68477c5c4c6c8217554876309cf1fe3d9adc67f71f024f9e5ccfd74d6ad1bee (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."customer_id" as "customer_id",
   base."region" as "region",
   base."amount" as "amount"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
