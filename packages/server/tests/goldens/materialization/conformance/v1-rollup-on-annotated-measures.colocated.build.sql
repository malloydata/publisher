-- conformance: v1 rollup on annotated measures [colocated]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders__preagg__region__c697d298
-- sourceID: orders__preagg__region__c697d298@<pkg>/m.malloy.preagg.malloy
-- connection: duckdb
-- sourceEntityId: c7034313cf1cf3de0f065239fc65bad7f748a4cfc3c779bc5b3a1c355ad335ee
-- compiler key: c7034313cf1cf3de0f065239fc65bad7f748a4cfc3c779bc5b3a1c355ad335ee (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   (COUNT(1)) as "n__partial",
   (COALESCE(SUM(base."amount"),0)) as "total__partial"
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

-- ==== <pkg>/m.malloy.preagg.malloy
##! experimental { persistence composite_sources }
import { orders__preagg_base is orders } from "m.malloy"

#@ persist
source: orders__preagg__region__c697d298 is orders__preagg_base -> {
  group_by:
    region
  aggregate:
    n__partial is n
    total__partial is total
} extend {
  measure:
    #(doc) "Orders"
    n is n__partial.sum()
    # currency
    #(doc) "Revenue"
    total is total__partial.sum()
}

source: orders is compose(orders__preagg__region__c697d298, orders__preagg_base)
