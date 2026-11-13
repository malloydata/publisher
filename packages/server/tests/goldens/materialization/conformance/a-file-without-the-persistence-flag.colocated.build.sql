-- conformance: a file without the persistence flag [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_nf
-- sourceID: daily_nf@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 1e5484a3823a4e65fdf5c864a38250e45a7f191789529e2d3f1fd9032fb84b00
-- compiler key: 1e5484a3823a4e65fdf5c864a38250e45a7f191789529e2d3f1fd9032fb84b00 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
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

-- generated files

-- ==== <pkg>/m.malloy
##! experimental.persistence
import "raw.malloy"

#@ persist name="daily_nf"
source: daily_nf is orders_raw -> {
  group_by: region, order_date
  aggregate: total is amount.sum()
} extend {
  measure: grand is total.sum()
}
