-- feature: rolling windows: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source between
-- sourceID: between@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 5e2703da57e062e1d9937960233fffd5f765019c6495e5879eeca4f87d84e11d
-- compiler key: 5e2703da57e062e1d9937960233fffd5f765019c6495e5879eeca4f87d84e11d (the sourceEntityId)
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
WHERE (base."created_at">=((LOCALTIMESTAMP - INTERVAL (60) day)))
AND (true)

-- ==== source older
-- sourceID: older@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: a8734965f12f8dbc5ae5774d10e6be2534b32640f161be6d1f2435f15f7cc21e
-- compiler key: a8734965f12f8dbc5ae5774d10e6be2534b32640f161be6d1f2435f15f7cc21e (the sourceEntityId)
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
WHERE true

-- ==== source older_inherited
-- sourceID: older_inherited@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: a8734965f12f8dbc5ae5774d10e6be2534b32640f161be6d1f2435f15f7cc21e
-- compiler key: a8734965f12f8dbc5ae5774d10e6be2534b32640f161be6d1f2435f15f7cc21e (the sourceEntityId)
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
WHERE true

-- ==== source recent
-- sourceID: recent@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 06f65db1905c6c613f21f6fa5ce13753ab3247676cb1dea31390e0370d60d5c2
-- compiler key: 06f65db1905c6c613f21f6fa5ce13753ab3247676cb1dea31390e0370d60d5c2 (the sourceEntityId)
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
WHERE base."created_at">=((LOCALTIMESTAMP - INTERVAL (30) day))

-- generated files

-- ==== <pkg>/m.malloy.0.materialize-build.malloy
import "raw.malloy"

// Lower bound: rows newer than a moving instant.
#(materialize) storage="lake"
source: recent is orders_raw extend {
  where: created_at >= now - 30 days
  measure: n is count()
}

// Upper bound: rows older than a moving instant.
#(materialize) storage="lake"
source: older is orders_raw extend {
  where: created_at < now - 30 days
  measure: n is count()
}

// Two-sided.
#(materialize) storage="lake"
source: between is orders_raw extend {
  where: true
  measure: n is count()
}

source: older_base is orders_raw extend {
  where: now - 30 days > created_at
}

// Inherited from a base.
#(materialize) storage="lake"
source: older_inherited is older_base extend {
  measure: n is count()
}

-- ==== <pkg>/m.malloy.1.materialize-build.malloy
import "raw.malloy"

// Lower bound: rows newer than a moving instant.
#(materialize) storage="lake"
source: recent is orders_raw extend {
  where: created_at >= now - 30 days
  measure: n is count()
}

// Upper bound: rows older than a moving instant.
#(materialize) storage="lake"
source: older is orders_raw extend {
  where: true
  measure: n is count()
}

// Two-sided.
#(materialize) storage="lake"
source: between is orders_raw extend {
  where: created_at >= now - 60 days and created_at < now - 30 days
  measure: n is count()
}

source: older_base is orders_raw extend {
  where: now - 30 days > created_at
}

// Inherited from a base.
#(materialize) storage="lake"
source: older_inherited is older_base extend {
  measure: n is count()
}

-- ==== <pkg>/m.malloy.2.materialize-build.malloy
import "raw.malloy"

// Lower bound: rows newer than a moving instant.
#(materialize) storage="lake"
source: recent is orders_raw extend {
  where: created_at >= now - 30 days
  measure: n is count()
}

// Upper bound: rows older than a moving instant.
#(materialize) storage="lake"
source: older is orders_raw extend {
  where: created_at < now - 30 days
  measure: n is count()
}

// Two-sided.
#(materialize) storage="lake"
source: between is orders_raw extend {
  where: created_at >= now - 60 days and created_at < now - 30 days
  measure: n is count()
}

source: older_base is orders_raw extend {
  where: true
}

// Inherited from a base.
#(materialize) storage="lake"
source: older_inherited is older_base extend {
  measure: n is count()
}

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { between__mat__base is between } from 'm.malloy.0.materialize-build.malloy'
import { older__mat__base is older } from 'm.malloy.1.materialize-build.malloy'
import { older_inherited__mat__base is older_inherited } from 'm.malloy.2.materialize-build.malloy'
import { recent__mat__base is recent } from 'm.malloy'

#@ persist name="between" storage="lake"
source: between__mat is between__mat__base -> {
  where: created_at >= now - 60 days
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

#@ persist name="older" storage="lake"
source: older__mat is older__mat__base -> {
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

#@ persist name="older_inherited" storage="lake"
source: older_inherited__mat is older_inherited__mat__base -> {
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

#@ persist name="recent" storage="lake"
source: recent__mat is recent__mat__base -> {
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
