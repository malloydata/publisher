-- conformance: givens and access filters [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source scoped
-- sourceID: scoped@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 439322f15aaa7b722da31ed15c9726c991147a06d9a3d3ee61b698b270b2f4f9
-- compiler key: 439322f15aaa7b722da31ed15c9726c991147a06d9a3d3ee61b698b270b2f4f9 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."region" as "region",
   base."status" as "status",
   base."amount" as "amount",
   base."order_date" as "order_date"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base

-- ==== source gated_ext
-- sourceID: gated_ext@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 439322f15aaa7b722da31ed15c9726c991147a06d9a3d3ee61b698b270b2f4f9
-- compiler key: 439322f15aaa7b722da31ed15c9726c991147a06d9a3d3ee61b698b270b2f4f9 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."region" as "region",
   base."status" as "status",
   base."amount" as "amount",
   base."order_date" as "order_date"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base

-- ==== source gated
-- sourceID: gated@<pkg>/m.malloy
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
