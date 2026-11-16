-- conformance: same-named sources in two models, no name= [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source summary
-- sourceID: summary@<pkg>/a.malloy
-- connection: duckdb
-- sourceEntityId: 2bc8e6b03947bd397184af804c21e9d894683327921d58b6b411e3faaec2465f
-- compiler key: 2bc8e6b03947bd397184af804c21e9d894683327921d58b6b411e3faaec2465f (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
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
  WHERE base."status"='placed'
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   base."total" as "total"
FROM __stage0 as base

-- ==== source summary
-- sourceID: summary@<pkg>/b.malloy
-- connection: duckdb
-- sourceEntityId: dd6c6173497ad72575666949d2ef30d23439f8ebbd55ed53dc98b1744ceab392
-- compiler key: dd6c6173497ad72575666949d2ef30d23439f8ebbd55ed53dc98b1744ceab392 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
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
  WHERE base."status"='shipped'
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   base."total" as "total"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/a.malloy.materialize.malloy
##! experimental.persistence
import { summary__mat__base is summary } from 'a.malloy'

#@ persist name="summary" storage="lake"
source: summary__mat is summary__mat__base -> {
  select:
    region,
    total
}

-- ==== <pkg>/b.malloy.materialize.malloy
##! experimental.persistence
import { summary__mat__base is summary } from 'b.malloy'

#@ persist name="summary" storage="lake"
source: summary__mat is summary__mat__base -> {
  select:
    region,
    total
}
