-- hammer/54-chained-build-error-redaction/cbe/cbe.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cbe/cbe.malloy
-- connection: orders_pg
-- sourceEntityId: 0beb870b10cae460bb65ba6ae5c84ca108926387326fd6d114b5940997ff3e01
-- compiler key: 0beb870b10cae460bb65ba6ae5c84ca108926387326fd6d114b5940997ff3e01 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cbe_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/cbe/cbe.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/cbe/cbe.malloy
-- sourceEntityId: ae4389d8da4e60c248915a38e6753a2e5f91541e549ba7485ced46b110d0cdd1
-- compiler key: ae4389d8da4e60c248915a38e6753a2e5f91541e549ba7485ced46b110d0cdd1 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.cbe_orders as base
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
