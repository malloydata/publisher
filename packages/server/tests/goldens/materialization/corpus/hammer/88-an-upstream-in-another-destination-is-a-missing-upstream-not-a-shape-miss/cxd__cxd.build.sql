-- hammer/88-an-upstream-in-another-destination-is-a-missing-upstream-not-a-shape-miss/cxd/cxd.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cxd/cxd.malloy
-- connection: orders_pg
-- sourceEntityId: b40cd8b36d53ed352c7c14a83f7d636b2b4e9a811891d765f1233163d38cf20c
-- compiler key: b40cd8b36d53ed352c7c14a83f7d636b2b4e9a811891d765f1233163d38cf20c (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cxd_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/cxd/cxd.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/cxd/cxd.malloy
-- sourceEntityId: 61d6958657f3c907c2e3332b7ad43d1aacf947575ed5394fef14a85fd1ae3ffa
-- compiler key: 61d6958657f3c907c2e3332b7ad43d1aacf947575ed5394fef14a85fd1ae3ffa (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.cxd_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     base."order_date" as "order_date",
     base."total_amount" as "total_amount"
  FROM __stage0 as base
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage1 as base
-- build SQL with every other planned source built:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     base."total_amount" as "total_amount"
  FROM built.daily as base
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage0 as base
