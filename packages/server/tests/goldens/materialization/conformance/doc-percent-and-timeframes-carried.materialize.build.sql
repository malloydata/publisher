-- conformance: doc, percent and timeframes carried [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source monthly
-- sourceID: monthly@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: f7dbab505c7424aa1adaf396d8d73d71fa879daf4555000bae1091204f49b988
-- compiler key: f7dbab505c7424aa1adaf396d8d73d71fa879daf4555000bae1091204f49b988 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     DATE_TRUNC('month', base."order_date") as "order_month",
     DATE_TRUNC('hour', base."created_at") as "created_hour",
     (DATE_TRUNC('week', (base."order_date" + INTERVAL '1' DAY)) - INTERVAL '1' DAY) as "order_week",
     base."region" as "region",
     COALESCE(SUM(base."amount"),0) as "total",
     AVG(base."discount") as "discount_rate"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
  GROUP BY 1,2,3,4
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   base."order_month" as "order_month",
   base."created_hour" as "created_hour",
   base."order_week" as "order_week",
   base."region" as "region",
   base."total" as "total",
   base."discount_rate" as "discount_rate"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { monthly__mat__base is monthly } from 'm.malloy'

#@ persist name="monthly" storage="lake"
source: monthly__mat is monthly__mat__base -> {
  select:
    order_month,
    created_hour,
    order_week,
    region,
    total,
    discount_rate
}
