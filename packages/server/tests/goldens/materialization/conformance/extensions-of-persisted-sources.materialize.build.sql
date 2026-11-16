-- conformance: extensions of persisted sources [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source base_daily
-- sourceID: base_daily@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 4a41560a6c4c879c97bca95b66a0e8edfe51021df18c872c73e362b154c0828f
-- compiler key: 4a41560a6c4c879c97bca95b66a0e8edfe51021df18c872c73e362b154c0828f (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."customer_id" as "customer_id",
     base."region" as "region",
     base."status" as "status",
     base."order_date" as "order_date",
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
  GROUP BY 1,2,3,4
  ORDER BY 4 desc NULLS LAST
)
SELECT 
   base."customer_id" as "customer_id",
   base."region" as "region",
   base."status" as "status",
   base."order_date" as "order_date",
   base."total" as "total",
   base."n" as "n"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { base_daily__mat__base is base_daily } from 'm.malloy'

#@ persist name="base_daily" storage="lake"
source: base_daily__mat is base_daily__mat__base -> {
  select:
    customer_id,
    region,
    status,
    order_date,
    total,
    n
}
