-- hammer/93-a-join-declared-on-a-stored-upstream-counts-as-reaching-what-it-joins/sjs/sjs.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_counts
-- sourceID: daily_counts@<hammer>/sjs/sjs.malloy
-- connection: orders_pg
-- sourceEntityId: 71cc56091a7ec0a6c3836309522697973643ada99bb92cc59b8ee766f8d309f2
-- compiler key: 71cc56091a7ec0a6c3836309522697973643ada99bb92cc59b8ee766f8d309f2 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "order_count"
FROM public.sjs_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source daily
-- sourceID: daily@<hammer>/sjs/sjs.malloy
-- connection: orders_pg
-- reads: daily_counts@<hammer>/sjs/sjs.malloy
-- sourceEntityId: 918af6b637f67065a4593d2e51addd406a55da2cab159f05d2026893e56681aa
-- compiler key: 918af6b637f67065a4593d2e51addd406a55da2cab159f05d2026893e56681aa (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.sjs_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/sjs/sjs.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/sjs/sjs.malloy
-- sourceEntityId: e67ca5a08bd3f520d38ae4e4351577558617247ac1d326f0a2f5ba5ba0a0d634
-- compiler key: e67ca5a08bd3f520d38ae4e4351577558617247ac1d326f0a2f5ba5ba0a0d634 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.sjs_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     base."order_date" as "order_date",
     COUNT(1) as "order_count"
  FROM public.sjs_orders as base
  GROUP BY 1
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total",
   COALESCE((
        SELECT SUM(a.val) as value
        FROM (
          SELECT UNNEST(list(distinct {key:c_0."order_date", val: CASE WHEN c_0."order_date" IS NOT NULL THEN c_0."order_count" END})) a
        )
      ),0) as "orders"
FROM __stage0 as base
 LEFT JOIN __stage1 AS c_0
  ON base."order_date"=c_0."order_date"
-- build SQL with every other planned source built:
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total",
   COALESCE((
        SELECT SUM(a.val) as value
        FROM (
          SELECT UNNEST(list(distinct {key:c_0."order_date", val: CASE WHEN c_0."order_date" IS NOT NULL THEN c_0."order_count" END})) a
        )
      ),0) as "orders"
FROM built.daily as base
 LEFT JOIN built.daily_counts AS c_0
  ON base."order_date"=c_0."order_date"
