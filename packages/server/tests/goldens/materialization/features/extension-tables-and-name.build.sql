-- feature: extension tables and name=: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source colocated_ext
-- sourceID: colocated_ext@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: bfd63fc0b64f3eb8051d97c6733d8cfd5603c84a195da169efa44f21f9f990a1
-- compiler key: bfd63fc0b64f3eb8051d97c6733d8cfd5603c84a195da169efa44f21f9f990a1 (the sourceEntityId)
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
WHERE base."status"='shipped'

-- ==== source stored
-- sourceID: stored@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 4c8462933bea4ad5a18f5d69a16f80e4a82f7387ef86f18ce8632d17f3c62de7
-- compiler key: 4c8462933bea4ad5a18f5d69a16f80e4a82f7387ef86f18ce8632d17f3c62de7 (the sourceEntityId)
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
   base."amount">100 as "is_big"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base

-- ==== source by_given
-- sourceID: by_given@<pkg>/m.malloy
-- connection: duckdb
-- reads: stored@<pkg>/m.malloy
-- sourceEntityId: 89346a5441a6acebb9ed864a77a0577a19037eaf9209176e639247880b52728a
-- compiler key: 89346a5441a6acebb9ed864a77a0577a19037eaf9209176e639247880b52728a (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
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
     base."amount">100 as "is_big"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
)
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
   base."is_big" as "is_big"
FROM __stage0 as base
WHERE true
-- build SQL with every other planned source built:
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
   base."is_big" as "is_big"
FROM built.stored as base
WHERE true

-- ==== source east
-- sourceID: east@<pkg>/m.malloy
-- connection: duckdb
-- reads: stored@<pkg>/m.malloy
-- sourceEntityId: 00d0e92abe259082227975f4087359ecee8908192038e4bd8b47020dd75a8d13
-- compiler key: 00d0e92abe259082227975f4087359ecee8908192038e4bd8b47020dd75a8d13 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
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
     base."amount">100 as "is_big"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
)
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
   base."is_big" as "is_big"
FROM __stage0 as base
WHERE base."region"='east'
-- build SQL with every other planned source built:
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
   base."is_big" as "is_big"
FROM built.stored as base
WHERE base."region"='east'

-- ==== source renamed
-- sourceID: renamed@<pkg>/m.malloy
-- connection: duckdb
-- reads: stored@<pkg>/m.malloy
-- sourceEntityId: d5200d694f4551cc166e0f821e6651d42d736db4be01c931f45b8a3fbb918bda
-- compiler key: d5200d694f4551cc166e0f821e6651d42d736db4be01c931f45b8a3fbb918bda (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
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
     base."amount">100 as "is_big"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
)
SELECT 
   base."customer_id" as "customer_id",
   base."region" as "region",
   base."status" as "status",
   base."amount" as "amount",
   base."discount" as "discount",
   base."order_date" as "order_date",
   base."created_at" as "created_at",
   base."order" as "order",
   base."is_big" as "is_big",
   base."id" as "order_id"
FROM __stage0 as base
-- build SQL with every other planned source built:
SELECT 
   base."customer_id" as "customer_id",
   base."region" as "region",
   base."status" as "status",
   base."amount" as "amount",
   base."discount" as "discount",
   base."order_date" as "order_date",
   base."created_at" as "created_at",
   base."order" as "order",
   base."is_big" as "is_big",
   base."id" as "order_id"
FROM built.stored as base

-- ==== source east_big
-- sourceID: east_big@<pkg>/m.malloy
-- connection: duckdb
-- reads: east@<pkg>/m.malloy
-- sourceEntityId: 42ab988725be5a10da59d4dfcfd9cf0eb6186ac3109d553bc8a362d8cd308dc5
-- compiler key: 42ab988725be5a10da59d4dfcfd9cf0eb6186ac3109d553bc8a362d8cd308dc5 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
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
     base."amount">100 as "is_big"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
)
, __stage1 AS (
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
     base."is_big" as "is_big"
  FROM __stage0 as base
  WHERE base."region"='east'
)
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
   base."is_big" as "is_big"
FROM __stage1 as base
WHERE (base."region"='east')
AND (base."is_big")
-- build SQL with every other planned source built:
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
   base."is_big" as "is_big"
FROM built.east as base
WHERE (base."region"='east')
AND (base."is_big")

-- generated files

-- ==== <pkg>/m.malloy.0.materialize-build.malloy
##! experimental.givens
import { stored__mat } from 'm.malloy.0.materialize-level.malloy'
given: REGION :: string is 'east'

import "raw.malloy"

source: orders is orders_raw extend {
  measure:
    n is count()
    total is amount.sum()
}

#(materialize) storage="lake" name="stored_t"
source: stored is stored__mat extend {
}

#(materialize) storage="lake" name="east_t"
source: east is stored extend {
  where: region = 'east'
}

#(materialize) storage="lake" name="east_big_t"
source: east_big is east extend {
  where: is_big
}

#(materialize) storage="lake" name="by_given_t"
source: by_given is stored extend {
  where: true
}

#(materialize) storage="lake" name="renamed_t"
source: renamed is stored extend {
  rename: order_id is id
  measure: id_total is order_id.sum()
}

#(materialize) name="colocated_ext_t"
source: colocated_ext is orders extend {
  where: status = 'shipped'
}

-- ==== <pkg>/m.malloy.0.materialize-level.malloy
##! experimental.persistence
import { colocated_ext__mat__base is colocated_ext } from 'm.malloy'
import { stored__mat__base is stored } from 'm.malloy'

#@ persist name="colocated_ext_t"
source: colocated_ext__mat is colocated_ext__mat__base -> {
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

#@ persist name="stored_t" storage="lake"
source: stored__mat is stored__mat__base -> {
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
    is_big
}

-- ==== <pkg>/m.malloy.1.materialize-build.malloy
##! experimental.givens
import { stored__mat } from 'm.malloy.0.materialize-level.malloy'
given: REGION :: string is 'east'

import "raw.malloy"

source: orders is orders_raw extend {
  measure:
    n is count()
    total is amount.sum()
}

#(materialize) storage="lake" name="stored_t"
source: stored is stored__mat extend {
}

#(materialize) storage="lake" name="east_t"
source: east is stored extend {
  where: region = 'east'
}

#(materialize) storage="lake" name="east_big_t"
source: east_big is east extend {
  where: is_big
}

#(materialize) storage="lake" name="by_given_t"
source: by_given is stored extend {
  where: region = $REGION
}

#(materialize) storage="lake" name="renamed_t"
source: renamed is stored extend {
  rename: order_id is id
  measure: id_total is order_id.sum()
}

#(materialize) name="colocated_ext_t"
source: colocated_ext is orders extend {
  where: status = 'shipped'
}

-- ==== <pkg>/m.malloy.1.materialize-level.malloy
##! experimental { persistence givens }
import { by_given__mat__base is by_given } from 'm.malloy.0.materialize-build.malloy'
import { east__mat__base is east } from 'm.malloy.1.materialize-build.malloy'
import { renamed__mat__base is renamed } from 'm.malloy.2.materialize-build.malloy'
import { REGION } from 'm.malloy'

#@ persist name="by_given_t" storage="lake"
source: by_given__mat is by_given__mat__base -> {
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
    is_big
} extend {
  where: region = $REGION
}

#@ persist name="east_t" storage="lake"
source: east__mat is east__mat__base -> {
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
    is_big
}

#@ persist name="renamed_t" storage="lake"
source: renamed__mat is renamed__mat__base -> {
  select:
    customer_id,
    region,
    status,
    amount,
    discount,
    order_date,
    created_at,
    order,
    is_big,
    order_id
}

-- ==== <pkg>/m.malloy.2.materialize-build.malloy
##! experimental.givens
import { stored__mat } from 'm.malloy.0.materialize-level.malloy'
given: REGION :: string is 'east'

import "raw.malloy"

source: orders is orders_raw extend {
  measure:
    n is count()
    total is amount.sum()
}

#(materialize) storage="lake" name="stored_t"
source: stored is stored__mat extend {
}

#(materialize) storage="lake" name="east_t"
source: east is stored extend {
  where: region = 'east'
}

#(materialize) storage="lake" name="east_big_t"
source: east_big is east extend {
  where: is_big
}

#(materialize) storage="lake" name="by_given_t"
source: by_given is stored extend {
  where: region = $REGION
}

#(materialize) storage="lake" name="renamed_t"
source: renamed is stored extend {
  rename: order_id is id
  measure: id_total is order_id.sum()
}

#(materialize) name="colocated_ext_t"
source: colocated_ext is orders extend {
  where: status = 'shipped'
}

-- ==== <pkg>/m.malloy.2.materialize-level.malloy
##! experimental.persistence
import { east_big__mat__base is east_big } from 'm.malloy.3.materialize-build.malloy'

#@ persist name="east_big_t" storage="lake"
source: east_big__mat is east_big__mat__base -> {
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
    is_big
}

-- ==== <pkg>/m.malloy.3.materialize-build.malloy
##! experimental.givens
import { east__mat } from 'm.malloy.1.materialize-level.malloy'
given: REGION :: string is 'east'

import "raw.malloy"

source: orders is orders_raw extend {
  measure:
    n is count()
    total is amount.sum()
}

#(materialize) storage="lake" name="stored_t"
source: stored is orders extend {
  dimension: is_big is amount > 100
}

#(materialize) storage="lake" name="east_t"
source: east is east__mat extend {
  where: region = 'east'
}

#(materialize) storage="lake" name="east_big_t"
source: east_big is east extend {
  where: is_big
}

#(materialize) storage="lake" name="by_given_t"
source: by_given is stored extend {
  where: region = $REGION
}

#(materialize) storage="lake" name="renamed_t"
source: renamed is stored extend {
  rename: order_id is id
  measure: id_total is order_id.sum()
}

#(materialize) name="colocated_ext_t"
source: colocated_ext is orders extend {
  where: status = 'shipped'
}

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { colocated_ext__mat, stored__mat } from 'm.malloy.0.materialize-level.malloy'
import { by_given__mat, east__mat, renamed__mat } from 'm.malloy.1.materialize-level.malloy'
import { east_big__mat } from 'm.malloy.2.materialize-level.malloy'
