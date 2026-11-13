-- hammer/72-chained-write-only-orchestrated/cwoo/cwoo.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cwoo/cwoo.malloy
-- connection: orders_pg
-- sourceEntityId: 134f396575f45ba626f6458a6e9a8d154838663b7dd985fcc55c80cd148060b2
-- compiler key: 134f396575f45ba626f6458a6e9a8d154838663b7dd985fcc55c80cd148060b2 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cwoo_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/cwoo/cwoo.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/cwoo/cwoo.malloy
-- sourceEntityId: b8118ef82b413810b326a69ec68f853a66c39c62f1d105cd59d043d1ade6485b
-- compiler key: b8118ef82b413810b326a69ec68f853a66c39c62f1d105cd59d043d1ade6485b (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.cwoo_orders as base
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
