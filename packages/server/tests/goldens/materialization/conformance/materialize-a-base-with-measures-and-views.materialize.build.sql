-- conformance: materialize: a base with measures and views [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders_m
-- sourceID: orders_m@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 3f4562aa4a00b1792d2a380e8e007d40f57d6d24f3a52d472b1298cd7a48742c
-- compiler key: 3f4562aa4a00b1792d2a380e8e007d40f57d6d24f3a52d472b1298cd7a48742c (the sourceEntityId)
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
   base."order" as "order",
   UPPER(base."region") as "zone"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { orders_m__mat__base is orders_m } from 'm.malloy'

#@ persist name="orders_m" storage="lake"
source: orders_m__mat is orders_m__mat__base -> {
  select:
    id,
    customer_id,
    region,
    status,
    amount,
    discount,
    order_date,
    created_at,
    order,
    zone
}
