-- hammer/21-orchestrator-manifest-bind/orch/orch.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/orch/orch.malloy
-- connection: orders_pg
-- sourceEntityId: 6ff8713b2f8abc8accde2ec19f10d41d8be8f9dd25397ab79d13702713580309
-- compiler key: 6ff8713b2f8abc8accde2ec19f10d41d8be8f9dd25397ab79d13702713580309 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.orch_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
