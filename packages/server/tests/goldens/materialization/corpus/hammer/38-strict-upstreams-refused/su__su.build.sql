-- hammer/38-strict-upstreams-refused/su/su.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/su/su.malloy
-- connection: orders_pg
-- sourceEntityId: 71cccaa18633fc6cb2e7e8f8bd82493b9c04ebd94ff75ace97bd7dd37ac4a749
-- compiler key: 71cccaa18633fc6cb2e7e8f8bd82493b9c04ebd94ff75ace97bd7dd37ac4a749 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.su_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/su/su.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/su/su.malloy
-- sourceEntityId: 732ca4509f2c7f60a5a2850d829750c349838172937fe60d7633d2b44c0e1e31
-- compiler key: 732ca4509f2c7f60a5a2850d829750c349838172937fe60d7633d2b44c0e1e31 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.su_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage0 as base
-- build SQL with every other planned source built:
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM built.daily as base
