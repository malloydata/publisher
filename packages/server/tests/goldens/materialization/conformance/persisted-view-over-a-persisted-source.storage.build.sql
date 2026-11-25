-- conformance: persisted view over a persisted source [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source pv_base__by_region__view
-- sourceID: pv_base__by_region__view@<pkg>/m.malloy.persist-view.malloy
-- connection: duckdb
-- reads: pv_base@<pkg>/m.malloy
-- sourceEntityId: a634b528cb34b67c79aaa476de57cdfa5f64b1c47abd877843a927606a776a4b
-- compiler key: a634b528cb34b67c79aaa476de57cdfa5f64b1c47abd877843a927606a776a4b (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."id" as "id",
     base."region" as "region",
     base."status" as "status",
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
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source pv_base
-- sourceID: pv_base@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 8c98767eac2f2caa728cf31081e45f40c637955c2a62deaea2247369207b6792
-- compiler key: 8c98767eac2f2caa728cf31081e45f40c637955c2a62deaea2247369207b6792 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."region" as "region",
   base."status" as "status",
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

-- generated files

-- ==== <pkg>/m.malloy
##! experimental { persistence }
import "raw.malloy"

                                        
source: pv_base is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
  #@ persist name="pv_base_by_region" storage="lake"
  view: by_region is { group_by: region; aggregate: total }
}

-- ==== <pkg>/m.malloy.persist-view.malloy
##! experimental.persistence
import { pv_base__pv_base is pv_base } from "m.malloy"

#@ persist name="pv_base_by_region" storage="lake"
source: pv_base__by_region__view is pv_base__pv_base -> by_region
