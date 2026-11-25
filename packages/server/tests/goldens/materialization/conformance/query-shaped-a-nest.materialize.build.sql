-- conformance: query-shaped: a nest [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source nested
-- sourceID: nested@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 2dd1093d430137e8a6481be81760193ed55b86915c232e4d9ed5f68efd17443c
-- compiler key: 2dd1093d430137e8a6481be81760193ed55b86915c232e4d9ed5f68efd17443c (the sourceEntityId)
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
, __stage1 AS (
  SELECT
    "region__0" as "region",
    MAX(CASE WHEN group_set=0 THEN "total__0" END) as "total",
    COALESCE(LIST({
      "status": "status__1", 
      "n": "n__1"}  ORDER BY  "n__1" DESC NULLS LAST) FILTER (WHERE group_set=1),[]) as "by_status"
  FROM __stage0
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   base."total" as "total",
   base."by_status" as "by_status"
FROM __stage1 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { nested__mat__base is nested } from 'm.malloy'

#@ persist name="nested" storage="lake"
source: nested__mat is nested__mat__base -> {
  select:
    region,
    total,
    by_status
}
