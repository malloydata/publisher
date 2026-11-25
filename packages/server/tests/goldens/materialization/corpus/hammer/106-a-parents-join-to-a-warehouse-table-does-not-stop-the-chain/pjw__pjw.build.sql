-- hammer/106-a-parents-join-to-a-warehouse-table-does-not-stop-the-chain/pjw/pjw.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/pjw/pjw.malloy
-- connection: orders_pg
-- sourceEntityId: b4a5b0b921ff3cb12606008c0032f7e74c65840ec9a66f9566bdb79d0734cd09
-- compiler key: b4a5b0b921ff3cb12606008c0032f7e74c65840ec9a66f9566bdb79d0734cd09 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.pjw_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/pjw/pjw.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/pjw/pjw.malloy
-- sourceEntityId: 500818b9ebdb35cf6ebcfe1f8fa516748e0b1fff5571e337b78d8626ba86253a
-- compiler key: 500818b9ebdb35cf6ebcfe1f8fa516748e0b1fff5571e337b78d8626ba86253a (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     base."region_id" as "region_id",
     COALESCE(SUM(base."amount"),0) as "total"
  FROM public.pjw_orders as base
  GROUP BY 1,2
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   COALESCE(SUM(base."total"),0) as "grand"
FROM __stage0 as base
WHERE base."total">60
-- build SQL with every other planned source built:
SELECT 
   COALESCE(SUM(base."total"),0) as "grand"
FROM built.daily as base
WHERE base."total">60
