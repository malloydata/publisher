-- feature: lexer edges: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source 注文
-- sourceID: 注文@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: f82b7a7c65d7a942ab50f90ce47c7d11515bc1234214e9c8e799d4dfe78ac652
-- compiler key: f82b7a7c65d7a942ab50f90ce47c7d11515bc1234214e9c8e799d4dfe78ac652 (the sourceEntityId)
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
   base."region" as "地域"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base

-- ==== source café
-- sourceID: café@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 906722090c9ae623e7254ab13cf1d687ac6d844df492f8397cdae8f437d09bbe
-- compiler key: 906722090c9ae623e7254ab13cf1d687ac6d844df492f8397cdae8f437d09bbe (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COUNT(1) as "größe"
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

-- ==== source via_sql
-- sourceID: via_sql@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 2405df3b7488da9e3e7d9cdeedb73b54628640971bb8f1bb5af19c95e29c6ede
-- compiler key: 2405df3b7488da9e3e7d9cdeedb73b54628640971bb8f1bb5af19c95e29c6ede (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM (select * from (SELECT 
   base."region" as "region",
   base."amount" as "amount"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
)) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { `注文__mat__base` is `注文` } from 'm.malloy'

#@ persist name="orders_cjk" storage="lake"
source: `注文__mat` is `注文__mat__base` -> {
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
    `地域`
}
