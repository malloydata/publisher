-- feature: time zones: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source mat
-- sourceID: mat@<pkg>/m.malloy
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

-- ==== source wide
-- sourceID: wide@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: a55afd4939ad8e5de2177af551a7b36316f114ae00b27bc8af37a5e89910fb13
-- compiler key: a55afd4939ad8e5de2177af551a7b36316f114ae00b27bc8af37a5e89910fb13 (the sourceEntityId)
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
   customers_0."id" as "customers__id",
   customers_0."name" as "customers__name",
   customers_0."segment" as "customers__segment",
   customers_0."home_region" as "customers__home_region",
   customers_0."signup_date" as "customers__signup_date"
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

-- ==== source orders__rollup__m_region__75e7c0f9
-- sourceID: orders__rollup__m_region__75e7c0f9@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: 63ab64738419e7bc208520ea3386607248fa4a63e035a05092114b5fbc7085f7
-- compiler key: 63ab64738419e7bc208520ea3386607248fa4a63e035a05092114b5fbc7085f7 (the sourceEntityId)
-- build SQL:
SELECT 
   DATE_TRUNC('month', base."created_at") as "m",
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total__partial"
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
ORDER BY 1 desc NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { mat__mat__base is mat } from 'm.malloy'
import { wide__mat__base is wide } from 'm.malloy'
import { wide__mat__unique__customers__source is customers_raw } from 'raw.malloy'

#@ persist name="mat" storage="lake"
source: mat__mat is mat__mat__base -> {
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

#@ persist name="wide" storage="lake"
source: wide__mat is wide__mat__base -> {
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
    customers__id is customers.id,
    customers__name is customers.name,
    customers__segment is customers.segment,
    customers__home_region is customers.home_region,
    customers__signup_date is customers.signup_date
}

query: wide__mat__unique__customers is wide__mat__unique__customers__source -> {
  where: id is not null
  group_by: id
  aggregate: matches is count()
  having: matches > 1
  limit: 1
}

-- ==== <pkg>/m.malloy.preaggregate.malloy
##! experimental.persistence
import { orders__rollup_base is orders } from "m.malloy"

#@ persist
source: orders__rollup__m_region__75e7c0f9 is orders__rollup_base -> {
  group_by:
    m
    region
  aggregate:
    total__partial is amount.sum()
}
