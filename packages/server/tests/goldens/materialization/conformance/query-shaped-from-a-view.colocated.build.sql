-- conformance: query-shaped: from a view [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source from_view
-- sourceID: from_view@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 4250ea814a3dff950753ae0c1d232c25465baaa30ddfe31fc97e7db31327e426
-- compiler key: 4250ea814a3dff950753ae0c1d232c25465baaa30ddfe31fc97e7db31327e426 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   base."status" as "status",
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
GROUP BY 1,2
ORDER BY 3 desc NULLS LAST
