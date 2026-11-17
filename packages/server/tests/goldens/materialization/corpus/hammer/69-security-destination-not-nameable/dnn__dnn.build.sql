-- hammer/69-security-destination-not-nameable/dnn/dnn.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/dnn/dnn.malloy
-- connection: orders_pg
-- sourceEntityId: 224a8a45bbc2e48ace830e4e4699494f6a5bc0682f507637ddc645892f086bb7
-- compiler key: 224a8a45bbc2e48ace830e4e4699494f6a5bc0682f507637ddc645892f086bb7 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.dnn_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
