-- hammer/34-cross-worker-refreshed-manifest/xwr/xwr.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/xwr/xwr.malloy
-- connection: orders_pg
-- sourceEntityId: e817bcb8836ad7b5260b6688de97cee4a20dfef0d95184cfe88f139bf7b87660
-- compiler key: e817bcb8836ad7b5260b6688de97cee4a20dfef0d95184cfe88f139bf7b87660 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.xwr_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source monthly
-- sourceID: monthly@<hammer>/xwr/xwr.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/xwr/xwr.malloy
-- sourceEntityId: 19dcbac0cc53075f77122e67318fb52df0e92c9284c3c65323aff959e818d9f4
-- compiler key: 19dcbac0cc53075f77122e67318fb52df0e92c9284c3c65323aff959e818d9f4 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.xwr_orders as base
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
