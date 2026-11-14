-- feature: pair tables: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source customers_blocks
-- sourceID: customers_blocks@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: c058478f6fd7ba6db74b5a5bfc1e6e83509095e4f9088595a8f3d8f110b275a9
-- compiler key: c058478f6fd7ba6db74b5a5bfc1e6e83509095e4f9088595a8f3d8f110b275a9 (the sourceEntityId)
-- build SQL:
SELECT * FROM ((SELECT 
   base."id" as "id",
   base."name" as "name",
   base."segment" as "segment",
   base."home_region" as "home_region",
   base."signup_date" as "signup_date",
   orders_0."id" as "orders__id",
   orders_0."customer_id" as "orders__customer_id",
   orders_0."region" as "orders__region",
   orders_0."status" as "orders__status",
   orders_0."amount" as "orders__amount",
   orders_0."discount" as "orders__discount",
   orders_0."order_date" as "orders__order_date",
   orders_0."created_at" as "orders__created_at",
   orders_0."order" as "orders__order",
   lines_0."line_id" as "orders__lines__line_id",
   lines_0."order_id" as "orders__lines__order_id",
   lines_0."sku" as "orders__lines__sku",
   lines_0."qty" as "orders__lines__qty",
   CASE WHEN false THEN 0 ELSE NULL END as "big_orders__id",
   CASE WHEN false THEN 0 ELSE NULL END as "big_orders__customer_id",
   CAST(NULL AS varchar) as "big_orders__region",
   CAST(NULL AS varchar) as "big_orders__status",
   CASE WHEN false THEN 0 ELSE NULL END as "big_orders__amount",
   CASE WHEN false THEN 0 ELSE NULL END as "big_orders__discount",
   CAST(NULL AS date) as "big_orders__order_date",
   CAST(NULL AS timestamp) as "big_orders__created_at",
   CASE WHEN false THEN 0 ELSE NULL END as "big_orders__order",
   0 as "__pair_lane",
   (ROW_NUMBER() OVER(PARTITION BY (base."id")  ORDER BY  base."id" ASC NULLS LAST ))=1 as "__parent_row",
   (ROW_NUMBER() OVER(PARTITION BY (base."id"), (orders_0."id")  ORDER BY  base."id" ASC NULLS LAST ))=1 as "__child_row"
FROM (
  SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
         CAST(signup_date AS DATE) AS signup_date
  FROM read_csv('data/customers.csv', header=true, all_varchar=true)
) as base
 LEFT JOIN (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) AS orders_0
  ON orders_0."customer_id"=base."id"
 LEFT JOIN (
  SELECT CAST(line_id AS BIGINT) AS line_id, CAST(order_id AS BIGINT) AS order_id,
         sku, CAST(qty AS BIGINT) AS qty
  FROM read_csv('data/lines.csv', header=true, all_varchar=true)
) AS lines_0
  ON lines_0."order_id"=orders_0."id"
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
ORDER BY 1 ASC NULLS LAST
)) AS __pair_lane_0
UNION ALL
SELECT * FROM ((SELECT 
   base."id" as "id",
   base."name" as "name",
   base."segment" as "segment",
   base."home_region" as "home_region",
   base."signup_date" as "signup_date",
   CASE WHEN false THEN 0 ELSE NULL END as "orders__id",
   CASE WHEN false THEN 0 ELSE NULL END as "orders__customer_id",
   CAST(NULL AS varchar) as "orders__region",
   CAST(NULL AS varchar) as "orders__status",
   CASE WHEN false THEN 0 ELSE NULL END as "orders__amount",
   CASE WHEN false THEN 0 ELSE NULL END as "orders__discount",
   CAST(NULL AS date) as "orders__order_date",
   CAST(NULL AS timestamp) as "orders__created_at",
   CASE WHEN false THEN 0 ELSE NULL END as "orders__order",
   CASE WHEN false THEN 0 ELSE NULL END as "orders__lines__line_id",
   CASE WHEN false THEN 0 ELSE NULL END as "orders__lines__order_id",
   CAST(NULL AS varchar) as "orders__lines__sku",
   CASE WHEN false THEN 0 ELSE NULL END as "orders__lines__qty",
   big_orders_0."id" as "big_orders__id",
   big_orders_0."customer_id" as "big_orders__customer_id",
   big_orders_0."region" as "big_orders__region",
   big_orders_0."status" as "big_orders__status",
   big_orders_0."amount" as "big_orders__amount",
   big_orders_0."discount" as "big_orders__discount",
   big_orders_0."order_date" as "big_orders__order_date",
   big_orders_0."created_at" as "big_orders__created_at",
   big_orders_0."order" as "big_orders__order",
   1 as "__pair_lane",
   (ROW_NUMBER() OVER(PARTITION BY (base."id")  ORDER BY  base."id" ASC NULLS LAST ))=1 as "__parent_row",
   (ROW_NUMBER() OVER(PARTITION BY (base."id"), (big_orders_0."id")  ORDER BY  base."id" ASC NULLS LAST ))=1 as "__child_row"
FROM (
  SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
         CAST(signup_date AS DATE) AS signup_date
  FROM read_csv('data/customers.csv', header=true, all_varchar=true)
) as base
 LEFT JOIN (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) AS big_orders_0
  ON big_orders_0."customer_id"=base."id"
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
ORDER BY 1 ASC NULLS LAST
)) AS __pair_lane_1

-- ==== source customers_path
-- sourceID: customers_path@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 86beef7e3f90b49adcf4a9b57106ab241236ddb36df39ba354767030f794e373
-- compiler key: 86beef7e3f90b49adcf4a9b57106ab241236ddb36df39ba354767030f794e373 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."name" as "name",
   base."segment" as "segment",
   base."home_region" as "home_region",
   base."signup_date" as "signup_date",
   orders_0."id" as "orders__id",
   orders_0."customer_id" as "orders__customer_id",
   orders_0."region" as "orders__region",
   orders_0."status" as "orders__status",
   orders_0."amount" as "orders__amount",
   orders_0."discount" as "orders__discount",
   orders_0."order_date" as "orders__order_date",
   orders_0."created_at" as "orders__created_at",
   orders_0."order" as "orders__order",
   lines_0."line_id" as "orders__lines__line_id",
   lines_0."order_id" as "orders__lines__order_id",
   lines_0."sku" as "orders__lines__sku",
   lines_0."qty" as "orders__lines__qty",
   (ROW_NUMBER() OVER(PARTITION BY (base."id")  ORDER BY  base."id" ASC NULLS LAST ))=1 as "__parent_row",
   (ROW_NUMBER() OVER(PARTITION BY (base."id"), (orders_0."id")  ORDER BY  base."id" ASC NULLS LAST ))=1 as "__child_row"
FROM (
  SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
         CAST(signup_date AS DATE) AS signup_date
  FROM read_csv('data/customers.csv', header=true, all_varchar=true)
) as base
 LEFT JOIN (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) AS orders_0
  ON orders_0."customer_id"=base."id"
 LEFT JOIN (
  SELECT CAST(line_id AS BIGINT) AS line_id, CAST(order_id AS BIGINT) AS order_id,
         sku, CAST(qty AS BIGINT) AS qty
  FROM read_csv('data/lines.csv', header=true, all_varchar=true)
) AS lines_0
  ON lines_0."order_id"=orders_0."id"
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
ORDER BY 1 ASC NULLS LAST

-- ==== source orders_pair
-- sourceID: orders_pair@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 19d80b436ce31978acec04b3d99ce0adea58a8c832e8f450c8d8052ce14b7016
-- compiler key: 19d80b436ce31978acec04b3d99ce0adea58a8c832e8f450c8d8052ce14b7016 (the sourceEntityId)
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
   lines_0."line_id" as "lines__line_id",
   lines_0."order_id" as "lines__order_id",
   lines_0."sku" as "lines__sku",
   lines_0."qty" as "lines__qty",
   (ROW_NUMBER() OVER(PARTITION BY (base."id")  ORDER BY  base."id" ASC NULLS LAST ))=1 as "__parent_row"
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
  SELECT CAST(line_id AS BIGINT) AS line_id, CAST(order_id AS BIGINT) AS order_id,
         sku, CAST(qty AS BIGINT) AS qty
  FROM read_csv('data/lines.csv', header=true, all_varchar=true)
) AS lines_0
  ON lines_0."order_id"=base."id"
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
ORDER BY 1 ASC NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { customers_blocks__mat__base is customers_blocks } from 'm.malloy'
import { customers_blocks__mat__pair_key__source is customers_blocks } from 'm.malloy'
import { customers_blocks__mat__pair_key__orders__source is orders_with_lines } from 'm.malloy'
import { customers_blocks__mat__pair_leaf_key__orders__lines__source is lines } from 'm.malloy'
import { customers_blocks__mat__pair_leaf_key__big_orders__source is orders_raw } from 'raw.malloy'
import { customers_path__mat__base is customers_path } from 'm.malloy'
import { customers_path__mat__pair_key__source is customers_path } from 'm.malloy'
import { customers_path__mat__pair_key__orders__source is orders_with_lines } from 'm.malloy'
import { customers_path__mat__pair_leaf_key__orders__lines__source is lines } from 'm.malloy'
import { orders_pair__mat__base is orders_pair } from 'm.malloy'
import { orders_pair__mat__pair_key__source is orders_pair } from 'm.malloy'
import { orders_pair__mat__pair_leaf_key__lines__source is lines } from 'm.malloy'

#@ persist name="customers_blocks" storage="lake"
source: customers_blocks__mat is duckdb.sql("""
SELECT * FROM (%{ customers_blocks__mat__base -> {
  group_by:
    id,
    name,
    segment,
    home_region,
    signup_date,
    orders__id is orders.id,
    orders__customer_id is orders.customer_id,
    orders__region is orders.region,
    orders__status is orders.status,
    orders__amount is orders.amount,
    orders__discount is orders.discount,
    orders__order_date is orders.order_date,
    orders__created_at is orders.created_at,
    orders__order is orders.order,
    orders__lines__line_id is orders.lines.line_id,
    orders__lines__order_id is orders.lines.order_id,
    orders__lines__sku is orders.lines.sku,
    orders__lines__qty is orders.lines.qty,
    big_orders__id is pick 0 when false else null,
    big_orders__customer_id is pick 0 when false else null,
    big_orders__region is null::string,
    big_orders__status is null::string,
    big_orders__amount is pick 0 when false else null,
    big_orders__discount is pick 0 when false else null,
    big_orders__order_date is null::date,
    big_orders__created_at is null::timestamp,
    big_orders__order is pick 0 when false else null,
    __pair_lane is 0
  calculate:
    __parent_row is row_number() { partition_by: id } = 1
    __child_row is row_number() { partition_by: id, orders__id } = 1
  order_by: id
} }) AS __pair_lane_0
UNION ALL
SELECT * FROM (%{ customers_blocks__mat__base -> {
  group_by:
    id,
    name,
    segment,
    home_region,
    signup_date,
    orders__id is pick 0 when false else null,
    orders__customer_id is pick 0 when false else null,
    orders__region is null::string,
    orders__status is null::string,
    orders__amount is pick 0 when false else null,
    orders__discount is pick 0 when false else null,
    orders__order_date is null::date,
    orders__created_at is null::timestamp,
    orders__order is pick 0 when false else null,
    orders__lines__line_id is pick 0 when false else null,
    orders__lines__order_id is pick 0 when false else null,
    orders__lines__sku is null::string,
    orders__lines__qty is pick 0 when false else null,
    big_orders__id is big_orders.id,
    big_orders__customer_id is big_orders.customer_id,
    big_orders__region is big_orders.region,
    big_orders__status is big_orders.status,
    big_orders__amount is big_orders.amount,
    big_orders__discount is big_orders.discount,
    big_orders__order_date is big_orders.order_date,
    big_orders__created_at is big_orders.created_at,
    big_orders__order is big_orders.order,
    __pair_lane is 1
  calculate:
    __parent_row is row_number() { partition_by: id } = 1
    __child_row is row_number() { partition_by: id, big_orders__id } = 1
  order_by: id
} }) AS __pair_lane_1
""")

query: customers_blocks__mat__pair_key is customers_blocks__mat__pair_key__source -> {
  group_by: id
  aggregate: matches is count()
} -> {
  where: matches > 1 or id is null
  select: id
  limit: 1
}

query: customers_blocks__mat__pair_key__orders is customers_blocks__mat__pair_key__orders__source -> {
  group_by: id
  aggregate: matches is count()
} -> {
  where: matches > 1 or id is null
  select: id
  limit: 1
}

query: customers_blocks__mat__pair_leaf_key__orders__lines is customers_blocks__mat__pair_leaf_key__orders__lines__source -> {
  group_by: line_id
  aggregate: matches is count()
} -> {
  where: matches > 1 or line_id is null
  select: line_id
  limit: 1
}

query: customers_blocks__mat__pair_leaf_key__big_orders is customers_blocks__mat__pair_leaf_key__big_orders__source -> {
  group_by: id
  aggregate: matches is count()
} -> {
  where: matches > 1 or id is null
  select: id
  limit: 1
}

#@ persist name="customers_path" storage="lake"
source: customers_path__mat is customers_path__mat__base -> {
  group_by:
    id,
    name,
    segment,
    home_region,
    signup_date,
    orders__id is orders.id,
    orders__customer_id is orders.customer_id,
    orders__region is orders.region,
    orders__status is orders.status,
    orders__amount is orders.amount,
    orders__discount is orders.discount,
    orders__order_date is orders.order_date,
    orders__created_at is orders.created_at,
    orders__order is orders.order,
    orders__lines__line_id is orders.lines.line_id,
    orders__lines__order_id is orders.lines.order_id,
    orders__lines__sku is orders.lines.sku,
    orders__lines__qty is orders.lines.qty
  calculate:
    __parent_row is row_number() { partition_by: id } = 1
    __child_row is row_number() { partition_by: id, orders__id } = 1
  order_by: id
}

query: customers_path__mat__pair_key is customers_path__mat__pair_key__source -> {
  group_by: id
  aggregate: matches is count()
} -> {
  where: matches > 1 or id is null
  select: id
  limit: 1
}

query: customers_path__mat__pair_key__orders is customers_path__mat__pair_key__orders__source -> {
  group_by: id
  aggregate: matches is count()
} -> {
  where: matches > 1 or id is null
  select: id
  limit: 1
}

query: customers_path__mat__pair_leaf_key__orders__lines is customers_path__mat__pair_leaf_key__orders__lines__source -> {
  group_by: line_id
  aggregate: matches is count()
} -> {
  where: matches > 1 or line_id is null
  select: line_id
  limit: 1
}

#@ persist name="orders_pair" storage="lake"
source: orders_pair__mat is orders_pair__mat__base -> {
  group_by:
    id,
    customer_id,
    region,
    status,
    amount,
    discount,
    order_date,
    created_at,
    order,
    lines__line_id is lines.line_id,
    lines__order_id is lines.order_id,
    lines__sku is lines.sku,
    lines__qty is lines.qty
  calculate:
    __parent_row is row_number() { partition_by: id } = 1
  order_by: id
}

query: orders_pair__mat__pair_key is orders_pair__mat__pair_key__source -> {
  group_by: id
  aggregate: matches is count()
} -> {
  where: matches > 1 or id is null
  select: id
  limit: 1
}

query: orders_pair__mat__pair_leaf_key__lines is orders_pair__mat__pair_leaf_key__lines__source -> {
  group_by: line_id
  aggregate: matches is count()
} -> {
  where: matches > 1 or line_id is null
  select: line_id
  limit: 1
}
