-- hammer/31-reference-manifest-reuse/rmr/rmr.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/rmr/rmr.malloy
-- connection: orders_pg
-- sourceEntityId: 017934443568616422765ac9b053f6cabf4ff431ee837c61857c5e61dcf7ddb2
-- compiler key: 017934443568616422765ac9b053f6cabf4ff431ee837c61857c5e61dcf7ddb2 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.rmr_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/rmr/rmr.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/rmr/rmr.malloy
-- sourceEntityId: 0a989cfe00f2150b8f35859f194234f2d6530c5a415ab81bc3b71189689c0653
-- compiler key: 0a989cfe00f2150b8f35859f194234f2d6530c5a415ab81bc3b71189689c0653 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.rmr_orders as base
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
