-- conformance: a classified persisted source [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source classified
-- sourceID: classified@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 38ce5f3db9c65fb2c67e1dd4e7e5d6d52666932eabcb5ddc9b2ad503b52cfeeb
-- compiler key: 38ce5f3db9c65fb2c67e1dd4e7e5d6d52666932eabcb5ddc9b2ad503b52cfeeb (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."customer_id" as "customer_id",
     base."region" as "region",
     COALESCE(SUM(base."amount"),0) as "total"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
           region, status,
           CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
           CAST(order_date AS DATE) AS order_date,
           CAST(created_at AS TIMESTAMP) AS created_at,
           CAST("order" AS BIGINT) AS "order"
    FROM read_csv('data/orders.csv', header=true, all_varchar=true)
  ) as base
  GROUP BY 1,2
  ORDER BY 3 desc NULLS LAST
)
SELECT 
   base."customer_id" as "customer_id",
   base."region" as "region",
   base."total" as "total"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { classified__mat__base is classified } from 'm.malloy'

#@ persist name="classified" storage="lake"
source: classified__mat is classified__mat__base -> {
  select:
    customer_id,
    region,
    total
}
