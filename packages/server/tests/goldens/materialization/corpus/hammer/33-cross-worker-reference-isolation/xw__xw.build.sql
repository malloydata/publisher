-- hammer/33-cross-worker-reference-isolation/xw/xw.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/xw/xw.malloy
-- connection: orders_pg
-- sourceEntityId: 8e99eae2dfd733051a9767e484852ba40f78dd166a4247e193aad132a5440c72
-- compiler key: 8e99eae2dfd733051a9767e484852ba40f78dd166a4247e193aad132a5440c72 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.xw_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/xw/xw.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/xw/xw.malloy
-- sourceEntityId: 460ff9c2486601f774e392e0179afccbc75a3b086b084c401d171c7668f6adde
-- compiler key: 460ff9c2486601f774e392e0179afccbc75a3b086b084c401d171c7668f6adde (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.xw_orders as base
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
