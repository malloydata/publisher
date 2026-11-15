-- feature: given terms: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source both
-- sourceID: both@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 7f3ca981cd5837467a36b0de530a39e858e03410cc504ddb78d84cb82a51c2ef
-- compiler key: 7f3ca981cd5837467a36b0de530a39e858e03410cc504ddb78d84cb82a51c2ef (the sourceEntityId)
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
WHERE (true)
AND (true)

-- ==== source by_base_dimension
-- sourceID: by_base_dimension@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 8a90c7803a9db184f74207c08bfb8859bc49671e7ee90198f5e0f53a55e2be30
-- compiler key: 8a90c7803a9db184f74207c08bfb8859bc49671e7ee90198f5e0f53a55e2be30 (the sourceEntityId)
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
WHERE (true)
AND ((base."amount">=0))

-- ==== source inherited
-- sourceID: inherited@<pkg>/m.malloy
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

-- ==== source own
-- sourceID: own@<pkg>/m.malloy
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

-- generated files

-- ==== <pkg>/m.malloy.0.materialize-build.malloy
##! experimental { givens access_modifiers }
given:
  REGION :: string is 'north'
  MIN_AMT :: number is 0

import "raw.malloy"

// Its own given-reading where:.
#(materialize) storage="lake"
source: own is orders_raw extend {
  where: region = $REGION
  measure:
    n is count()
    total is amount.sum()
}

source: base is orders_raw extend {
  where: true
  dimension: at_least is amount >= $MIN_AMT
}

// A where: it inherits from its base.
#(materialize) storage="lake"
source: inherited is base extend {
  measure:
    n is count()
    total is amount.sum()
}

// A where: on a base's given-reading dimension.
#(materialize) storage="lake"
source: by_base_dimension is base extend {
  where: at_least
  measure: n is count()
}

// Both: an inherited term and one of its own.
#(materialize) storage="lake"
source: both is base extend {
  where: true
  measure: n is count()
}

-- ==== <pkg>/m.malloy.1.materialize-build.malloy
##! experimental { givens access_modifiers }
given:
  REGION :: string is 'north'
  MIN_AMT :: number is 0

import "raw.malloy"

// Its own given-reading where:.
#(materialize) storage="lake"
source: own is orders_raw extend {
  where: region = $REGION
  measure:
    n is count()
    total is amount.sum()
}

source: base is orders_raw extend {
  where: true
  dimension: at_least is amount >= $MIN_AMT
}

// A where: it inherits from its base.
#(materialize) storage="lake"
source: inherited is base extend {
  measure:
    n is count()
    total is amount.sum()
}

// A where: on a base's given-reading dimension.
#(materialize) storage="lake"
source: by_base_dimension is base extend {
  where: at_least
  measure: n is count()
}

// Both: an inherited term and one of its own.
#(materialize) storage="lake"
source: both is base extend {
  where: amount >= $MIN_AMT
  measure: n is count()
}

-- ==== <pkg>/m.malloy.2.materialize-build.malloy
##! experimental { givens access_modifiers }
given:
  REGION :: string is 'north'
  MIN_AMT :: number is 0

import "raw.malloy"

// Its own given-reading where:.
#(materialize) storage="lake"
source: own is orders_raw extend {
  where: region = $REGION
  measure:
    n is count()
    total is amount.sum()
}

source: base is orders_raw extend {
  where: true
  dimension: at_least is amount >= $MIN_AMT
}

// A where: it inherits from its base.
#(materialize) storage="lake"
source: inherited is base extend {
  measure:
    n is count()
    total is amount.sum()
}

// A where: on a base's given-reading dimension.
#(materialize) storage="lake"
source: by_base_dimension is base extend {
  where: at_least
  measure: n is count()
}

// Both: an inherited term and one of its own.
#(materialize) storage="lake"
source: both is base extend {
  where: amount >= $MIN_AMT
  measure: n is count()
}

-- ==== <pkg>/m.malloy.3.materialize-build.malloy
##! experimental { givens access_modifiers }
given:
  REGION :: string is 'north'
  MIN_AMT :: number is 0

import "raw.malloy"

// Its own given-reading where:.
#(materialize) storage="lake"
source: own is orders_raw extend {
  where: true
  measure:
    n is count()
    total is amount.sum()
}

source: base is orders_raw extend {
  where: region = $REGION
  dimension: at_least is amount >= $MIN_AMT
}

// A where: it inherits from its base.
#(materialize) storage="lake"
source: inherited is base extend {
  measure:
    n is count()
    total is amount.sum()
}

// A where: on a base's given-reading dimension.
#(materialize) storage="lake"
source: by_base_dimension is base extend {
  where: at_least
  measure: n is count()
}

// Both: an inherited term and one of its own.
#(materialize) storage="lake"
source: both is base extend {
  where: amount >= $MIN_AMT
  measure: n is count()
}

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental { persistence givens }
import { both__mat__base is both } from 'm.malloy.0.materialize-build.malloy'
import { by_base_dimension__mat__base is by_base_dimension } from 'm.malloy.1.materialize-build.malloy'
import { inherited__mat__base is inherited } from 'm.malloy.2.materialize-build.malloy'
import { own__mat__base is own } from 'm.malloy.3.materialize-build.malloy'
import { MIN_AMT, REGION } from 'm.malloy'

#@ persist name="both" storage="lake"
source: both__mat is both__mat__base -> {
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
} extend {
  where: region = $REGION
  where: amount >= $MIN_AMT
}

#@ persist name="by_base_dimension" storage="lake"
source: by_base_dimension__mat is by_base_dimension__mat__base -> {
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
} extend {
  where: region = $REGION
}

#@ persist name="inherited" storage="lake"
source: inherited__mat is inherited__mat__base -> {
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
} extend {
  where: region = $REGION
}

#@ persist name="own" storage="lake"
source: own__mat is own__mat__base -> {
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
} extend {
  where: region = $REGION
}
