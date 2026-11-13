-- conformance: a file without the persistence flag [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_nf
-- sourceID: daily_nf@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: e3515d44e6a5a2414323ff7eac03f531a4226ef892dce76fea0ee087c30a2a88
-- compiler key: e3515d44e6a5a2414323ff7eac03f531a4226ef892dce76fea0ee087c30a2a88 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     base."order_date" as "order_date",
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
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   base."order_date" as "order_date",
   base."total" as "total"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { daily_nf__mat__base is daily_nf } from 'm.malloy'

#@ persist name="daily_nf" storage="lake"
source: daily_nf__mat is daily_nf__mat__base -> {
  select:
    region,
    order_date,
    total
}
