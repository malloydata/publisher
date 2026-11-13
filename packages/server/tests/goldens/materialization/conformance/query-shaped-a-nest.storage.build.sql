-- conformance: query-shaped: a nest [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source nested
-- sourceID: nested@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 4365b8d3eda178e6b33df6bcaad07414941c816f5a3fac47ef192273eea6c732
-- compiler key: 4365b8d3eda178e6b33df6bcaad07414941c816f5a3fac47ef192273eea6c732 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT
    group_set,
    base."region" as "region__0",
    CASE WHEN group_set=0 THEN
      COALESCE(SUM(base."amount"),0)
      END as "total__0",
    CASE WHEN group_set=1 THEN
      base."status"
      END as "status__1",
    CASE WHEN group_set=1 THEN
      COUNT(1)
      END as "n__1"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
  CROSS JOIN (SELECT UNNEST(GENERATE_SERIES(0,1,1)) as group_set  ) as group_set
  GROUP BY 1,2,4
)
SELECT
  "region__0" as "region",
  MAX(CASE WHEN group_set=0 THEN "total__0" END) as "total",
  COALESCE(LIST({
    "status": "status__1", 
    "n": "n__1"}  ORDER BY  "n__1" DESC NULLS LAST) FILTER (WHERE group_set=1),[]) as "by_status"
FROM __stage0
GROUP BY 1
ORDER BY 2 desc NULLS LAST
