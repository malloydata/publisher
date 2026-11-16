-- conformance: materialize: colocated and storage= in one model [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source mz_dim
-- sourceID: mz_dim@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: d97e22c42cac6734195188c4a825c6947067a776f362f519546a5e1190f1d900
-- compiler key: d97e22c42cac6734195188c4a825c6947067a776f362f519546a5e1190f1d900 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."name" as "name",
   base."segment" as "segment",
   base."home_region" as "home_region",
   base."signup_date" as "signup_date"
FROM (
  SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
         CAST(signup_date AS DATE) AS signup_date
  FROM read_csv('data/customers.csv', header=true, all_varchar=true)
) as base

-- ==== source mz_fact
-- sourceID: mz_fact@<pkg>/m.malloy
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

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { mz_dim__mat__base is mz_dim } from 'm.malloy'
import { mz_fact__mat__base is mz_fact } from 'm.malloy'

#@ persist name="mz_dim_colocated"
source: mz_dim__mat is mz_dim__mat__base -> {
  select:
    id,
    name,
    segment,
    home_region,
    signup_date
}

#@ persist name="mz_fact" storage="lake"
source: mz_fact__mat is mz_fact__mat__base -> {
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
