-- conformance: materialize: a refined source [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source east_west
-- sourceID: east_west@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 66844921999eecf1f00874459a3b65a47e4947dad08dbb52d961e11f2951bbf8
-- compiler key: 66844921999eecf1f00874459a3b65a47e4947dad08dbb52d961e11f2951bbf8 (the sourceEntityId)
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
   DATE_TRUNC('month', base."order_date") as "order_month",
   base."amount">100 as "big"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
WHERE (base."region"='east') or (base."region"='west')

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { east_west__mat__base is east_west } from 'm.malloy'

#@ persist name="east_west" storage="lake"
source: east_west__mat is east_west__mat__base -> {
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
    order_month,
    big
}
