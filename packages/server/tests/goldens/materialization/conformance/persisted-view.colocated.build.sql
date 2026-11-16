-- conformance: persisted view [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source ov__by_region__view
-- sourceID: ov__by_region__view@<pkg>/m.malloy.persist-view.malloy
-- connection: duckdb
-- sourceEntityId: 35c99c4e12ad2cdcfa61ed24112545dcfd2904216c718bf57a90cee803f16533
-- compiler key: 35c99c4e12ad2cdcfa61ed24112545dcfd2904216c718bf57a90cee803f16533 (the sourceEntityId)
-- build SQL:
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
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy.persist-view.malloy
##! experimental.persistence
import { ov__pv_base is ov } from "m.malloy"

#@ persist name="ov_by_region"
source: ov__by_region__view is ov__pv_base -> by_region
