-- hammer/06-operator-generational/dop/dop.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/dop/dop.malloy
-- connection: orders_pg
-- sourceEntityId: a59a147de8f0a047c344f1070b60790ca7a015faa8881e635961634fa9266e6d
-- compiler key: a59a147de8f0a047c344f1070b60790ca7a015faa8881e635961634fa9266e6d (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.dop_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
