-- conformance: joined persisted source from another file [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source customer_dim
-- sourceID: customer_dim@<pkg>/dims.malloy
-- connection: duckdb
-- sourceEntityId: d14b4a5efb2857d27e71fe2eaae8c5206671492506854fc37a6bfa2c53f2528e
-- compiler key: d14b4a5efb2857d27e71fe2eaae8c5206671492506854fc37a6bfa2c53f2528e (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."segment" as "segment",
   base."home_region" as "home_region"
FROM (
  SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
         CAST(signup_date AS DATE) AS signup_date
  FROM read_csv('data/customers.csv', header=true, all_varchar=true)
) as base

-- ==== source order_facts
-- sourceID: order_facts@<pkg>/facts.malloy
-- connection: duckdb
-- reads: customer_dim@<pkg>/dims.malloy
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
