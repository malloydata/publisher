-- feature: preaggregate rollups and a join to a rollup: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source lines__rollup__order_id__906dde44
-- sourceID: lines__rollup__order_id__906dde44@<pkg>/m.malloy.preaggregate.malloy
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

-- ==== source lines__rollup__order_id_sku__c5895963
-- sourceID: lines__rollup__order_id_sku__c5895963@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: f07ea8bc9c40274198d8c7f091d311ec84a30c92a625d6aded00c4abb951ad0c
-- compiler key: f07ea8bc9c40274198d8c7f091d311ec84a30c92a625d6aded00c4abb951ad0c (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."sku" as "sku",
   COUNT(1) as "line_n__partial"
FROM (
  SELECT CAST(line_id AS BIGINT) AS line_id, CAST(order_id AS BIGINT) AS order_id,
         sku, CAST(qty AS BIGINT) AS qty
  FROM read_csv('data/lines.csv', header=true, all_varchar=true)
) as base
GROUP BY 1,2
ORDER BY 3 desc NULLS LAST

-- ==== source orders__rollup__region__409e9efb
-- sourceID: orders__rollup__region__409e9efb@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: 8ee590c7caa5a8e27b612492f2abec261baa65e65ea067328c7af3a037c1effc
-- compiler key: 8ee590c7caa5a8e27b612492f2abec261baa65e65ea067328c7af3a037c1effc (the sourceEntityId)
-- build SQL:
SELECT 
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
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source orders__rollup__m_region__7877d8b1
-- sourceID: orders__rollup__m_region__7877d8b1@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: 14db554405415a132affe4fd532e2a9bb42a117547176c6284f76a93f28d2356
-- compiler key: 14db554405415a132affe4fd532e2a9bb42a117547176c6284f76a93f28d2356 (the sourceEntityId)
-- build SQL:
SELECT 
   DATE_TRUNC('month', base."created_at") as "m",
   base."region" as "region",
   COUNT(1) as "n__partial"
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

-- ==== <pkg>/m.malloy.preaggregate.malloy
##! experimental.persistence
import { lines__rollup_base is lines } from "m.malloy"
import { orders__rollup_base is orders } from "m.malloy"

#@ persist
source: lines__rollup__order_id__906dde44 is lines__rollup_base -> {
  group_by:
    order_id
  aggregate:
    qty_total__partial is qty.sum()
}

#@ persist
source: lines__rollup__order_id_sku__c5895963 is lines__rollup_base -> {
  group_by:
    order_id
    sku
  aggregate:
    line_n__partial is count()
}

#@ persist
source: orders__rollup__region__409e9efb is orders__rollup_base -> {
  group_by:
    region
  aggregate:
    total__partial is amount.sum()
}

#@ persist storage="lake"
source: orders__rollup__m_region__7877d8b1 is orders__rollup_base -> {
  group_by:
    m
    region
  aggregate:
    n__partial is count()
}
