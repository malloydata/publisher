-- conformance: multi-line annotations [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source annotated
-- sourceID: annotated@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: e6445a23f015408d62c8f05d74c67fb93f6c6445d24521d1b1d4a41fd0a4b924
-- compiler key: e6445a23f015408d62c8f05d74c67fb93f6c6445d24521d1b1d4a41fd0a4b924 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   DATE_TRUNC('month', base."order_date") as "order_month",
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
ORDER BY 2 desc NULLS LAST
