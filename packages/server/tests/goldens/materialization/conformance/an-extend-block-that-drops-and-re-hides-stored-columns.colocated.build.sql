-- conformance: an extend block that drops and re-hides stored columns [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source dropped
-- sourceID: dropped@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 8c98767eac2f2caa728cf31081e45f40c637955c2a62deaea2247369207b6792
-- compiler key: 8c98767eac2f2caa728cf31081e45f40c637955c2a62deaea2247369207b6792 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."region" as "region",
   base."status" as "status",
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

-- ==== source rehidden
-- sourceID: rehidden@<pkg>/m.malloy
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
