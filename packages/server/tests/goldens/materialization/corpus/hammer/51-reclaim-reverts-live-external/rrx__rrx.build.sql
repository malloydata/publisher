-- hammer/51-reclaim-reverts-live-external/rrx/rrx.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/rrx/rrx.malloy
-- connection: orders_pg
-- sourceEntityId: 0c270e50a9800488e1de0e5139be478ba0f595223cdd0c91379d41d529d062e0
-- compiler key: 0c270e50a9800488e1de0e5139be478ba0f595223cdd0c91379d41d529d062e0 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.rrx_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
