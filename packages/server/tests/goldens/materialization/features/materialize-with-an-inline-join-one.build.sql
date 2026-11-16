-- feature: materialize with an inline join_one: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders_wide
-- sourceID: orders_wide@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: e21cb38810563b21fbe28f4348e65671976629e516095208d031f9f3263acbf1
-- compiler key: e21cb38810563b21fbe28f4348e65671976629e516095208d031f9f3263acbf1 (the sourceEntityId)
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
   customers_0."home_region" as "customer_region",
   customers_0."id" as "customers__id",
   customers_0."name" as "customers__name",
   customers_0."segment" as "customers__segment",
   customers_0."home_region" as "customers__home_region",
   customers_0."signup_date" as "customers__signup_date",
   (UPPER(customers_0."name")) as "customers__loud_name"
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
) AS customers_0
  ON customers_0."id"=base."customer_id"

-- ==== source orders_wide_local
-- sourceID: orders_wide_local@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: af106e366ef6b6a3cc9c6879b9ee78042dd3107bb9cbc7a746ad4a9f07863223
-- compiler key: af106e366ef6b6a3cc9c6879b9ee78042dd3107bb9cbc7a746ad4a9f07863223 (the sourceEntityId)
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
   customers_0."home_region" as "customer_region",
   customers_0."segment" as "customer_segment",
   customers_0."id" as "customers__id",
   customers_0."name" as "customers__name",
   customers_0."segment" as "customers__segment",
   customers_0."home_region" as "customers__home_region",
   customers_0."signup_date" as "customers__signup_date",
   (UPPER(customers_0."name")) as "customers__loud_name"
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
) AS customers_0
  ON customers_0."id"=base."customer_id"

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { orders_wide__mat__base is orders_wide } from 'm.malloy'
import { orders_wide__mat__unique__customers__source is customers } from 'm.malloy'
import { orders_wide_local__mat__base is orders_wide_local } from 'm.malloy'
import { orders_wide_local__mat__unique__customers__source is customers } from 'm.malloy'

#@ persist name="orders_wide" storage="lake"
source: orders_wide__mat is orders_wide__mat__base -> {
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
    customer_region,
    customers__id is customers.id,
    customers__name is customers.name,
    customers__segment is customers.segment,
    customers__home_region is customers.home_region,
    customers__signup_date is customers.signup_date,
    customers__loud_name is customers.loud_name
}

query: orders_wide__mat__unique__customers is orders_wide__mat__unique__customers__source -> {
  where: id is not null
  group_by: id
  aggregate: matches is count()
  having: matches > 1
  limit: 1
}

#@ persist name="orders_wide_local"
source: orders_wide_local__mat is orders_wide_local__mat__base -> {
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
    customer_region,
    customer_segment,
    customers__id is customers.id,
    customers__name is customers.name,
    customers__segment is customers.segment,
    customers__home_region is customers.home_region,
    customers__signup_date is customers.signup_date,
    customers__loud_name is customers.loud_name
}

query: orders_wide_local__mat__unique__customers is orders_wide_local__mat__unique__customers__source -> {
  where: id is not null
  group_by: id
  aggregate: matches is count()
  having: matches > 1
  limit: 1
}
