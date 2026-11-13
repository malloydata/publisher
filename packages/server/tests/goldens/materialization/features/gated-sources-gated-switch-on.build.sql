-- feature: gated sources, gated switch on: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source gated_mat
-- sourceID: gated_mat@<pkg>/m.malloy
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

-- ==== source locked_mat
-- sourceID: locked_mat@<pkg>/m.malloy
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

-- ==== source by_column__rollup__region_status__5f704d70
-- sourceID: by_column__rollup__region_status__5f704d70@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: 39eda9310e4be81c33824b2ad31f408ba67139cd58a825e6023c8abbd56e0d60
-- compiler key: 39eda9310e4be81c33824b2ad31f408ba67139cd58a825e6023c8abbd56e0d60 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   base."status" as "status",
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
ORDER BY 3 desc NULLS LAST

-- ==== source by_lock__rollup__status__eb4380b4
-- sourceID: by_lock__rollup__status__eb4380b4@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: 938641dc81f1620e036cdeb2408fe5eb9c87bb833bf074a38a84d1ae3406ec20
-- compiler key: 938641dc81f1620e036cdeb2408fe5eb9c87bb833bf074a38a84d1ae3406ec20 (the sourceEntityId)
-- build SQL:
SELECT 
   base."status" as "status",
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
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { gated_mat__mat__base is gated_mat } from 'm.malloy'
import { locked_mat__mat__base is locked_mat } from 'm.malloy'

#@ persist name="gated_mat" storage="lake"
source: gated_mat__mat is gated_mat__mat__base -> {
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

#@ persist name="locked_mat" storage="lake"
source: locked_mat__mat is locked_mat__mat__base -> {
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
import { by_column__rollup_base is by_column } from "m.malloy"
import { by_lock__rollup_base is by_lock } from "m.malloy"

#@ persist
source: by_column__rollup__region_status__5f704d70 is by_column__rollup_base -> {
  group_by:
    region
    status
  aggregate:
    total__partial is amount.sum()
}

#@ persist
source: by_lock__rollup__status__eb4380b4 is by_lock__rollup_base -> {
  group_by:
    status
  aggregate:
    total__partial is amount.sum()
}
