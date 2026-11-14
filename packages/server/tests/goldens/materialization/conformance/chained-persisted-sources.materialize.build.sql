-- conformance: chained persisted sources [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders_base
-- sourceID: orders_base@<pkg>/base.malloy
-- connection: duckdb
-- sourceEntityId: 8c2bf9a494a8b47b9c6c6a775e60743cfd4d9aff10c10303af8020ffa8b5b370
-- compiler key: 8c2bf9a494a8b47b9c6c6a775e60743cfd4d9aff10c10303af8020ffa8b5b370 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     base."status" as "status",
     COALESCE(SUM(base."amount"),0) as "total_amount"
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
   base."region" as "region",
   base."status" as "status",
   base."total_amount" as "total_amount"
FROM __stage0 as base

-- ==== source orders_rollup
-- sourceID: orders_rollup@<pkg>/base.malloy
-- connection: duckdb
-- sourceEntityId: 885c3a22adcd8bbaf839648f46134db6cfa303fb7775d64d4823cc2c419da9fe
-- compiler key: 885c3a22adcd8bbaf839648f46134db6cfa303fb7775d64d4823cc2c419da9fe (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     base."status" as "status",
     COALESCE(SUM(base."amount"),0) as "total_amount"
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
, __stage1 AS (
  SELECT 
     base."status" as "status",
     COUNT(1) as "regions",
     COALESCE(SUM(base."total_amount"),0) as "t"
  FROM __stage0 as base
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."status" as "status",
   base."regions" as "regions",
   base."t" as "t"
FROM __stage1 as base

-- ==== source orders_rollup_x
-- sourceID: orders_rollup_x@<pkg>/rollup.malloy
-- connection: duckdb
-- sourceEntityId: 8c121a419b30f0213986a811a8456baaf58a05e2036b4f6d49e49c490f2cb390
-- compiler key: 8c121a419b30f0213986a811a8456baaf58a05e2036b4f6d49e49c490f2cb390 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."region" as "region",
     base."status" as "status",
     COALESCE(SUM(base."amount"),0) as "total_amount"
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
, __stage1 AS (
  SELECT 
     base."region" as "region",
     COUNT(1) as "statuses",
     COALESCE(SUM(base."total_amount"),0) as "t"
  FROM __stage0 as base
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."region" as "region",
   base."statuses" as "statuses",
   base."t" as "t"
FROM __stage1 as base

-- generated files

-- ==== <pkg>/base.malloy.materialize.malloy
##! experimental.persistence
import { orders_base__mat__base is orders_base } from 'base.malloy'
import { orders_rollup__mat__base is orders_rollup } from 'base.malloy'

#@ persist name="orders_base" storage="lake"
source: orders_base__mat is orders_base__mat__base -> {
  select:
    region,
    status,
    total_amount
}

#@ persist name="orders_rollup" storage="lake"
source: orders_rollup__mat is orders_rollup__mat__base -> {
  select:
    status,
    regions,
    t
}

-- ==== <pkg>/rollup.malloy.materialize.malloy
##! experimental.persistence
import { orders_rollup_x__mat__base is orders_rollup_x } from 'rollup.malloy'

#@ persist name="orders_rollup_x" storage="lake"
source: orders_rollup_x__mat is orders_rollup_x__mat__base -> {
  select:
    region,
    statuses,
    t
}
