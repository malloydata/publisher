-- hammer/90-an-intermediate-joining-two-stored-sources-is-still-built-from-them/ctp/ctp.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/ctp/ctp.malloy
-- connection: orders_pg
-- sourceEntityId: 19419b266dd98e1b3a815a0f79507e1dd5b022eccc5358e978519ed2b333168c
-- compiler key: 19419b266dd98e1b3a815a0f79507e1dd5b022eccc5358e978519ed2b333168c (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.ctp_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source joined
-- sourceID: joined@<hammer>/ctp/ctp.malloy
-- connection: orders_pg
-- sourceEntityId: 19419b266dd98e1b3a815a0f79507e1dd5b022eccc5358e978519ed2b333168c
-- compiler key: 19419b266dd98e1b3a815a0f79507e1dd5b022eccc5358e978519ed2b333168c (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.ctp_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source daily_counts
-- sourceID: daily_counts@<hammer>/ctp/ctp.malloy
-- connection: orders_pg
-- sourceEntityId: 8ecb0ba1fb30213898e124664fed5956b906fc4692213f71ae06f4bac64442fe
-- compiler key: 8ecb0ba1fb30213898e124664fed5956b906fc4692213f71ae06f4bac64442fe (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "order_count"
FROM public.ctp_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/ctp/ctp.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/ctp/ctp.malloy, joined@<hammer>/ctp/ctp.malloy
-- sourceEntityId: 7e7d5b77f7d7730214932db51cbdb586c5c191831cb5f069207bfbf174bb74b2
-- compiler key: 7e7d5b77f7d7730214932db51cbdb586c5c191831cb5f069207bfbf174bb74b2 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.ctp_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     base."order_date" as "order_date",
     COUNT(1) as "order_count"
  FROM public.ctp_orders as base
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
FROM built.joined as base
 LEFT JOIN built.daily_counts AS c_0
  ON base."order_date"=c_0."order_date"
