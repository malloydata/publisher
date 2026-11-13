-- hammer/107-an-unused-join-to-a-supplied-stored-source-does-not-start-a-chain/ujs/ujs.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source counts
-- sourceID: counts@<hammer>/ujs/ujs.malloy
-- connection: orders_pg
-- sourceEntityId: add3bd758b374145f2c876a893a6a62fd7612b92707396f89bffcd6a343089c3
-- compiler key: add3bd758b374145f2c876a893a6a62fd7612b92707396f89bffcd6a343089c3 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "order_count"
FROM public.ujs_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source daily
-- sourceID: daily@<hammer>/ujs/ujs.malloy
-- connection: orders_pg
-- sourceEntityId: 6e9338a16903017e0cadcd9b6a07830981604ce9ed265c03e631eb990310418d
-- compiler key: 6e9338a16903017e0cadcd9b6a07830981604ce9ed265c03e631eb990310418d (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.ujs_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/ujs/ujs.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/ujs/ujs.malloy, counts@<hammer>/ujs/ujs.malloy
-- sourceEntityId: 9508c458098dd2677d0ad4064fe96e9f4d9d6f2166d5a287821d6faf3a1fcb05
-- compiler key: 9508c458098dd2677d0ad4064fe96e9f4d9d6f2166d5a287821d6faf3a1fcb05 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.ujs_orders as base
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
