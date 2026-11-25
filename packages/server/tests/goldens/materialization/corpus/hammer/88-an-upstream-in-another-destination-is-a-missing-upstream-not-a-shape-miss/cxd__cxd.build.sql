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
