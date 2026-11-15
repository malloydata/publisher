-- feature: a storage= rollup read through a join_many: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders
-- sourceID: orders@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: eae11fcde4b5b91f7571923a182628d686e43b4d324795e8e63d896255f534eb
-- compiler key: eae11fcde4b5b91f7571923a182628d686e43b4d324795e8e63d896255f534eb (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."customer_id" as "customer_id",
   base."region" as "region",
   base."status" as "status",
   base."amount" as "amount",
   base."discount" as "discount",
   base."order_date" as "order_date",
   base."created_at" as "created_at",
   base."order" as "order"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base

-- ==== source lines__rollup__order_id__7c483835
-- sourceID: lines__rollup__order_id__7c483835@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: ea7a32eb791f5d5e237d80571b0bd0dbde9460cd58ea650ba82d2045d4fabc91
-- compiler key: ea7a32eb791f5d5e237d80571b0bd0dbde9460cd58ea650ba82d2045d4fabc91 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   COALESCE(SUM(base."qty"),0) as "qty_total__partial"
FROM (
  SELECT CAST(line_id AS BIGINT) AS line_id, CAST(order_id AS BIGINT) AS order_id,
         sku, CAST(qty AS BIGINT) AS qty
  FROM read_csv('data/lines.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { orders__mat__base is orders } from 'm.malloy'

#@ persist name="orders" storage="lake"
source: orders__mat is orders__mat__base -> {
  select:
    id,
    customer_id,
    region,
    status,
    amount,
    discount,
    order_date,
    created_at,
    order
}

-- ==== <pkg>/m.malloy.preaggregate.malloy
##! experimental.persistence
import { lines__rollup_base is lines } from "m.malloy"

#@ persist storage="lake"
source: lines__rollup__order_id__7c483835 is lines__rollup_base -> {
  group_by:
    order_id
  aggregate:
    qty_total__partial is qty.sum()
}
