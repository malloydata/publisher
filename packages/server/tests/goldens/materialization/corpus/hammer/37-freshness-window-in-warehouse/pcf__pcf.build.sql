-- hammer/37-freshness-window-in-warehouse/pcf/pcf.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/pcf/pcf.malloy
-- connection: orders_pg
-- sourceEntityId: 0c4b42ebd3cd91935530187db099b00a7869b6c83f472fd7fcaa292fce8453c4
-- compiler key: 0c4b42ebd3cd91935530187db099b00a7869b6c83f472fd7fcaa292fce8453c4 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.pcf_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
