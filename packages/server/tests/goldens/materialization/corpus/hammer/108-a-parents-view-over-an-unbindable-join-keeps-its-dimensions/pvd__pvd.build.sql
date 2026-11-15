-- hammer/108-a-parents-view-over-an-unbindable-join-keeps-its-dimensions/pvd/pvd.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/pvd/pvd.malloy
-- connection: orders_pg
-- sourceEntityId: e566ea484bad7c76993f198ce5a584fb6c1a2fbe1a9a2d952e009d5e15dcf83b
-- compiler key: e566ea484bad7c76993f198ce5a584fb6c1a2fbe1a9a2d952e009d5e15dcf83b (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   base."region_id" as "region_id",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.pvd_orders as base
GROUP BY 1,2
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/pvd/pvd.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/pvd/pvd.malloy
-- sourceEntityId: 58ddf70d17900279a26de7a8d111b12552ffb04acfcb2f097841f992d74cec52
-- compiler key: 58ddf70d17900279a26de7a8d111b12552ffb04acfcb2f097841f992d74cec52 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     base."region_id" as "region_id",
     COALESCE(SUM(base."amount"),0) as "total"
  FROM public.pvd_orders as base
  GROUP BY 1,2
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   base."total">150 as "big",
   COALESCE(SUM(base."total"),0) as "grand"
FROM __stage0 as base
WHERE base."total">60
GROUP BY 1
ORDER BY 2 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   base."total">150 as "big",
   COALESCE(SUM(base."total"),0) as "grand"
FROM built.daily as base
WHERE base."total">60
GROUP BY 1
ORDER BY 2 desc NULLS LAST
