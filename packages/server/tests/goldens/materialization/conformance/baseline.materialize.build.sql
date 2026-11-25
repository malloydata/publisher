-- conformance: baseline [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 9bb02ad5a6d9a5376a2b6505820469260321bc0737501bc008f516da04b133b3
-- compiler key: 9bb02ad5a6d9a5376a2b6505820469260321bc0737501bc008f516da04b133b3 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total",
     COUNT(1) as "n"
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
   base."total" as "total",
   base."n" as "n",
   base."total">300 as "big"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { daily__mat__base is daily } from 'm.malloy'

#@ persist name="daily" storage="lake"
source: daily__mat is daily__mat__base -> {
  select:
    region,
    order_date,
    total,
    n,
    big
}
