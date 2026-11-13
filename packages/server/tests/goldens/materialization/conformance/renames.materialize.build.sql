-- conformance: renames [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_r
-- sourceID: daily_r@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 795f45fa66eea903bdfe67664522fbeb875ef94ed68b499f85d7e46fc9c8557f
-- compiler key: 795f45fa66eea903bdfe67664522fbeb875ef94ed68b499f85d7e46fc9c8557f (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "area",
     base."order_date" as "order_date",
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
)
SELECT 
   base."area" as "area",
   base."total" as "total",
   base."order_date" as "order_day"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { daily_r__mat__base is daily_r } from 'm.malloy'

#@ persist name="daily_r" storage="lake"
source: daily_r__mat is daily_r__mat__base -> {
  select:
    area,
    total,
    order_day
}
