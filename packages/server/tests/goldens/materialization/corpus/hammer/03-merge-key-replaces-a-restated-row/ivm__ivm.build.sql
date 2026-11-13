-- hammer/03-merge-key-replaces-a-restated-row/ivm/ivm.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source order_rows
-- sourceID: order_rows@<hammer>/ivm/ivm.malloy
-- connection: orders_pg
-- sourceEntityId: 0590287a429e95b9a7f1c85100e5da24bf537419ecef8ac84d2556d481d83ee3
-- compiler key: 0590287a429e95b9a7f1c85100e5da24bf537419ecef8ac84d2556d481d83ee3 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."batch" as "batch",
   COALESCE(SUM(base."amount"),0) as "amount_total"
FROM public.ivm_orders as base
GROUP BY 1,2
ORDER BY 3 desc NULLS LAST
