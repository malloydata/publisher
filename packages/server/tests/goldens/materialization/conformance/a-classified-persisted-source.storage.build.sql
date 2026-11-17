-- conformance: a classified persisted source [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source classified
-- sourceID: classified@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 7faf16dcee23ae32da24c3501e44774f7e91f7c67c06dac0e640e8c9937b12c2
-- compiler key: 7faf16dcee23ae32da24c3501e44774f7e91f7c67c06dac0e640e8c9937b12c2 (the sourceEntityId)
-- build SQL:
SELECT 
   base."customer_id" as "customer_id",
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
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
