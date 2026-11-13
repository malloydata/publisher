-- feature: persisted views: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__by_region__view
-- sourceID: orders__by_region__view@<pkg>/m.malloy.persist-view.malloy
-- connection: duckdb
-- sourceEntityId: 2fbc1e225d9431d76c336dc2997792c7d30e8ee6ca6c0a0aa793502eb420db5a
-- compiler key: 2fbc1e225d9431d76c336dc2997792c7d30e8ee6ca6c0a0aa793502eb420db5a (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COUNT(1) as "n",
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
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source orders__by_status__view
-- sourceID: orders__by_status__view@<pkg>/m.malloy.persist-view.malloy
-- connection: duckdb
-- sourceEntityId: 5581630f251fc69bee0819ff474a90427bd2332f845afe9236e0fc1b1bcee5e4
-- compiler key: 5581630f251fc69bee0819ff474a90427bd2332f845afe9236e0fc1b1bcee5e4 (the sourceEntityId)
-- build SQL:
SELECT 
   base."status" as "status",
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
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source orders__two_stage__view
-- sourceID: orders__two_stage__view@<pkg>/m.malloy.persist-view.malloy
-- connection: duckdb
-- sourceEntityId: b3fc9bd92fd43c859fd061bf8bd1333035de41bb7a77730d0fc9aa70cba70d88
-- compiler key: b3fc9bd92fd43c859fd061bf8bd1333035de41bb7a77730d0fc9aa70cba70d88 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
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
  GROUP BY 1
)
SELECT 
   base."region" as "region",
   base."n" as "n"
FROM __stage0 as base
WHERE base."n">1

-- generated files

-- ==== <pkg>/m.malloy
##! experimental.persistence
import "raw.malloy"

source: orders is orders_raw extend {
  measure:
    n is count()
    total is amount.sum()
  #@ persist storage="lake"
  view: by_region is { group_by: region; aggregate: n, total }
  #@ persist name="orders_by_status"
  view: by_status is { group_by: status; aggregate: n }
  #@ persist storage="lake"
  view: two_stage is { group_by: region; aggregate: n } -> { select: region, n; where: n > 1 }
  #@ persist storage="lake"
  view: recent is { where: created_at > now - 30 days; group_by: region; aggregate: n }
  view: plain is { group_by: status; aggregate: total }
}

source: north_orders is orders extend {
  where: region = 'north'
}

-- ==== <pkg>/m.malloy.persist-view.malloy
##! experimental.persistence
import { orders__pv_base is orders } from "m.malloy"

#@ persist storage="lake"
source: orders__by_region__view is orders__pv_base -> by_region

#@ persist name="orders_by_status"
source: orders__by_status__view is orders__pv_base -> by_status

#@ persist storage="lake"
source: orders__two_stage__view is orders__pv_base -> two_stage
