-- feature: hidden fields: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders
-- sourceID: orders@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: c215bd26b05cb26ff0cc5d7d83a4ed555895a01a0516d9ce4abfe890ba3be71c
-- compiler key: c215bd26b05cb26ff0cc5d7d83a4ed555895a01a0516d9ce4abfe890ba3be71c (the sourceEntityId)
-- build SQL:
SELECT 
   base."customer_id" as "customer_id",
   base."region" as "region",
   base."status" as "status",
   base."amount" as "amount",
   base."discount" as "discount",
   base."order_date" as "order_date",
   base."created_at" as "created_at",
   base."order" as "order",
   UPPER(base."region") as "region_code",
   base."amount"*2 as "secret",
   (UPPER(base."region"))='NORTH' as "north"
FROM 'data/orders.csv' as base

-- ==== source wide
-- sourceID: wide@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 2f6d3e57f13e9e62faf8122e60768b040c87df9c415fd3890f48c5d3915609ce
-- compiler key: 2f6d3e57f13e9e62faf8122e60768b040c87df9c415fd3890f48c5d3915609ce (the sourceEntityId)
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
FROM 'data/orders.csv' as base

-- generated files

-- ==== <pkg>/m.malloy.0.materialize-build.malloy
##! experimental.access_modifiers
##! experimental { givens access_modifiers }
given: GROUPS :: string[]

source: base is duckdb.table('data/orders.csv') include { private: id; internal: customer_id; public: * } extend {
  internal dimension: region_code is upper(region)
}

#(access_filter) region in $GROUPS
#(materialize) storage="lake"
source: orders is base extend {
  dimension: secret is amount * 2
  dimension: north is region_code = 'NORTH'
  measure:
    total is amount.sum()
    n is count()
    secret_total is secret.sum()
} include { public: * }

#(materialize) storage="lake"
source: wide is duckdb.table('data/orders.csv') include { private: amount; public: * } extend {
  measure: amt is amount.sum()
}

-- ==== <pkg>/m.malloy.1.materialize-build.malloy
##! experimental.access_modifiers
##! experimental { givens access_modifiers }
given: GROUPS :: string[]

source: base is duckdb.table('data/orders.csv') include { private: id; internal: customer_id; public: * } extend {
  internal dimension: region_code is upper(region)
}

#(access_filter) region in $GROUPS
#(materialize) storage="lake"
source: orders is base extend {
  private dimension: secret is amount * 2
  dimension: north is region_code = 'NORTH'
  measure:
    total is amount.sum()
    n is count()
    secret_total is secret.sum()
}

#(materialize) storage="lake"
source: wide is duckdb.table('data/orders.csv') include { public: amount; public: * } extend {
  measure: amt is amount.sum()
} include { public: * }

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { orders__mat__base is orders } from 'm.malloy.0.materialize-build.malloy'
import { wide__mat__base is wide } from 'm.malloy.1.materialize-build.malloy'

#@ persist name="orders" storage="lake"
source: orders__mat is orders__mat__base -> {
  select:
    customer_id,
    region,
    status,
    amount,
    discount,
    order_date,
    created_at,
    order,
    region_code,
    secret,
    north
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
    order
}
