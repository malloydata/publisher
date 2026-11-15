-- hammer/104-a-stored-parent-two-imports-away-is-stacked-on/tim/c.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/tim/a.malloy
-- connection: orders_pg
-- sourceEntityId: 77d1ee2d60220500033e17b15b7fe81dd68320356a8d460ec224f14100d5b3a6
-- compiler key: 77d1ee2d60220500033e17b15b7fe81dd68320356a8d460ec224f14100d5b3a6 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.tim_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source weekly
-- sourceID: weekly@<hammer>/tim/b.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/tim/a.malloy
-- sourceEntityId: 5ec6ceb979baf8d955182e10cc2e660837d7b414db4f0eaba3c68a51a87ece53
-- compiler key: 5ec6ceb979baf8d955182e10cc2e660837d7b414db4f0eaba3c68a51a87ece53 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.tim_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."total_amount"),0) as "weekly_total"
FROM __stage0 as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."total_amount"),0) as "weekly_total"
FROM built.daily as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source monthly
-- sourceID: monthly@<hammer>/tim/c.malloy
-- connection: orders_pg
-- reads: weekly@<hammer>/tim/b.malloy
-- sourceEntityId: 19a9328779b479a8922795b566ec8567a394b0b8983c3002cc38597017f06390
-- compiler key: 19a9328779b479a8922795b566ec8567a394b0b8983c3002cc38597017f06390 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.tim_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."total_amount"),0) as "weekly_total"
  FROM __stage0 as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   COALESCE(SUM(base."weekly_total"),0) as "grand_total"
FROM __stage1 as base
-- build SQL with every other planned source built:
SELECT 
   COALESCE(SUM(base."weekly_total"),0) as "grand_total"
FROM built.weekly as base
