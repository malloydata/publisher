-- conformance: persist with a rolling window [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source recent_body
-- sourceID: recent_body@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 107d1dd8522aeeba7588cf8afc6b8843d802c6e5edde9a26b5176861e3394e7e
-- compiler key: 107d1dd8522aeeba7588cf8afc6b8843d802c6e5edde9a26b5176861e3394e7e (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."region" as "region",
   base."amount" as "amount",
   base."created_at" as "created_at"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
WHERE base."created_at">=((LOCALTIMESTAMP - INTERVAL (3650) day))

-- ==== source recent_read
-- sourceID: recent_read@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: b8e1660417d152c47e6402d43cac5b3445a682feaa4410edbedb9791919eaee3
-- compiler key: b8e1660417d152c47e6402d43cac5b3445a682feaa4410edbedb9791919eaee3 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."region" as "region",
   base."amount" as "amount",
   base."created_at" as "created_at"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
