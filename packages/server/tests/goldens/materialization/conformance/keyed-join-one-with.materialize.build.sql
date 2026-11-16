-- conformance: keyed join_one with [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source cust
-- sourceID: cust@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: bf4286a91b893ea3bbeb2399ae99134474169fa1ea85a6dec9df7b2e2c845f59
-- compiler key: bf4286a91b893ea3bbeb2399ae99134474169fa1ea85a6dec9df7b2e2c845f59 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."id" as "id",
     base."segment" as "segment",
     base."home_region" as "home_region"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
           CAST(signup_date AS DATE) AS signup_date
    FROM read_csv('data/customers.csv', header=true, all_varchar=true)
  ) as base
)
SELECT 
   base."id" as "id",
   base."segment" as "segment",
   base."home_region" as "home_region"
FROM __stage0 as base

-- ==== source order_facts
-- sourceID: order_facts@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: ced04b2336c2cf76901620ecf674c5152d408fad1d03e7774cd61f272389778e
-- compiler key: ced04b2336c2cf76901620ecf674c5152d408fad1d03e7774cd61f272389778e (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."id" as "id",
     base."customer_id" as "customer_id",
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
)
SELECT 
   base."id" as "id",
   base."customer_id" as "customer_id",
   base."region" as "region",
   base."amount" as "amount"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { cust__mat__base is cust } from 'm.malloy'
import { order_facts__mat__base is order_facts } from 'm.malloy'

#@ persist name="cust" storage="lake"
source: cust__mat is cust__mat__base -> {
  select:
    id,
    segment,
    home_region
}

#@ persist name="order_facts" storage="lake"
source: order_facts__mat is order_facts__mat__base -> {
  select:
    id,
    customer_id,
    region,
    amount
}
