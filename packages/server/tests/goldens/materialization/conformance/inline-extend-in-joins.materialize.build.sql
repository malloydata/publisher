-- conformance: inline extend in joins [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source by_customer
-- sourceID: by_customer@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 71f8495dea14f21de55246d492a02ed7d2988d3cf10cf50157d9c94d37d2eb99
-- compiler key: 71f8495dea14f21de55246d492a02ed7d2988d3cf10cf50157d9c94d37d2eb99 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."customer_id" as "customer_id",
     UPPER(c_0."segment") as "seg_up",
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
   LEFT JOIN (
    SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
           CAST(signup_date AS DATE) AS signup_date
    FROM read_csv('data/customers.csv', header=true, all_varchar=true)
  ) AS c_0
    ON base."customer_id"=c_0."id"
  GROUP BY 1,2
  ORDER BY 3 desc NULLS LAST
)
SELECT 
   base."customer_id" as "customer_id",
   base."seg_up" as "seg_up",
   base."total" as "total",
   base."n" as "n"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { by_customer__mat__base is by_customer } from 'm.malloy'

#@ persist name="by_customer" storage="lake"
source: by_customer__mat is by_customer__mat__base -> {
  select:
    customer_id,
    seg_up,
    total,
    n
}
