-- hammer/99-a-dependency-only-the-compiler-reaches-is-still-strict/dcr/dcr.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source counts
-- sourceID: counts@<hammer>/dcr/dcr.malloy
-- connection: orders_pg
-- sourceEntityId: 9649ac4da618f5bf43b41ba01ae55ea8b1bc4b58c0b17c888f1f64ad1e93bb30
-- compiler key: 9649ac4da618f5bf43b41ba01ae55ea8b1bc4b58c0b17c888f1f64ad1e93bb30 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "order_count"
FROM public.dcr_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source daily
-- sourceID: daily@<hammer>/dcr/dcr.malloy
-- connection: orders_pg
-- reads: counts@<hammer>/dcr/dcr.malloy
-- sourceEntityId: 4670d7babf11d94680862c23003c6f6982bdf24affcc6e0a1c8e38be88a7d40a
-- compiler key: 4670d7babf11d94680862c23003c6f6982bdf24affcc6e0a1c8e38be88a7d40a (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.dcr_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/dcr/dcr.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/dcr/dcr.malloy
-- sourceEntityId: 7198faaf8672ba01060bb77f1ae48c2820d856f5a172a1d9962efdb8094e749e
-- compiler key: 7198faaf8672ba01060bb77f1ae48c2820d856f5a172a1d9962efdb8094e749e (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.dcr_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
, __stage1 AS (
  SELECT 
     base."order_date" as "order_date",
     COUNT(1) as "order_count"
  FROM public.dcr_orders as base
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
 LEFT JOIN built.counts AS c_0
  ON base."order_date"=c_0."order_date"
