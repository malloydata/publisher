-- conformance: materialize: a refined source with an inline join [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source enriched
-- sourceID: enriched@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 58628af49b4e48235c69d52e03ffa558cec96f5ed8cc67b5c1622215e2bf6909
-- compiler key: 58628af49b4e48235c69d52e03ffa558cec96f5ed8cc67b5c1622215e2bf6909 (the sourceEntityId)
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

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { enriched__mat__base is enriched } from 'm.malloy'

#@ persist name="enriched" storage="lake"
source: enriched__mat is enriched__mat__base -> {
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
