-- hammer/87-strict-still-refuses-an-upstream-never-built-through-an-intermediate/snb/snb.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/snb/snb.malloy
-- connection: orders_pg
-- sourceEntityId: 432945af4a72e0facd92c4998840b95f47d3ad1963211977be51bf52921314d5
-- compiler key: 432945af4a72e0facd92c4998840b95f47d3ad1963211977be51bf52921314d5 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.snb_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/snb/snb.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/snb/snb.malloy
-- sourceEntityId: 8f067150c16e8a60bcd4fb8d4db1a38763cdf8254bc941872e56f2a680971e9f
-- compiler key: 8f067150c16e8a60bcd4fb8d4db1a38763cdf8254bc941872e56f2a680971e9f (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.snb_orders as base
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
