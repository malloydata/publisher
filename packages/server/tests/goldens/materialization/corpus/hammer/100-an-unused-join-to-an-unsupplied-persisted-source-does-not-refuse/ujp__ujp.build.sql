-- hammer/100-an-unused-join-to-an-unsupplied-persisted-source-does-not-refuse/ujp/ujp.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source counts
-- sourceID: counts@<hammer>/ujp/ujp.malloy
-- connection: orders_pg
-- sourceEntityId: e0c8608d950cd1f4e05681cf328f0e8220dce2ad4498722787626adba3fb9772
-- compiler key: e0c8608d950cd1f4e05681cf328f0e8220dce2ad4498722787626adba3fb9772 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "order_count"
FROM public.ujp_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source daily
-- sourceID: daily@<hammer>/ujp/ujp.malloy
-- connection: orders_pg
-- sourceEntityId: 9e2946c24d93348533d887f4a1a76b78f6f3f4489f71eb0efec8d704340d4cd9
-- compiler key: 9e2946c24d93348533d887f4a1a76b78f6f3f4489f71eb0efec8d704340d4cd9 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.ujp_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/ujp/ujp.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/ujp/ujp.malloy, counts@<hammer>/ujp/ujp.malloy
-- sourceEntityId: 6296e0159a81ea1436716846404c383f0d729e331a186229bbea5e9755e0e784
-- compiler key: 6296e0159a81ea1436716846404c383f0d729e331a186229bbea5e9755e0e784 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.ujp_orders as base
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
