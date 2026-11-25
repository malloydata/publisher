-- conformance: persist over a zoned base [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source inherits_zone
-- sourceID: inherits_zone@<pkg>/m.malloy
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

-- ==== source sets_zone
-- sourceID: sets_zone@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 7087a145c021bb3abb6d843089ba74ee113f6841371adbd448bc5ce0667f9cd1
-- compiler key: 7087a145c021bb3abb6d843089ba74ee113f6841371adbd448bc5ce0667f9cd1 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."id" as "id",
     base."created_at" as "created_at",
     EXTRACT(hour FROM (base."created_at")::TIMESTAMPTZ AT TIME ZONE 'America/New_York') as "h",
     ((DATE_TRUNC('day', (base."created_at")::TIMESTAMPTZ AT TIME ZONE 'America/New_York')) AT TIME ZONE 'America/New_York')::TIMESTAMP as "d"
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
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."id" as "id",
   base."created_at" as "created_at",
   base."h" as "h",
   base."d" as "d"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { inherits_zone__mat__base is inherits_zone } from 'm.malloy'
import { sets_zone__mat__base is sets_zone } from 'm.malloy'

#@ persist name="inherits_zone" storage="lake"
source: inherits_zone__mat is inherits_zone__mat__base -> {
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

#@ persist name="sets_zone" storage="lake"
source: sets_zone__mat is sets_zone__mat__base -> {
  select:
    id,
    created_at,
    h,
    d
}
