-- hammer/103-a-stored-parent-reached-through-a-selective-import-is-stacked-on/sim/orders.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/sim/orders.malloy
-- connection: orders_pg
-- sourceEntityId: bca6f544d948e815fb8e735b47787915a4b52dbcc87e7d257e951af41ab7e5d3
-- compiler key: bca6f544d948e815fb8e735b47787915a4b52dbcc87e7d257e951af41ab7e5d3 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.sim_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source weekly
-- sourceID: weekly@<hammer>/sim/orders.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/sim/orders.malloy
-- sourceEntityId: cc7a8093a2fc454e407e504b031e3bf381b1d4004d6bc8b76e9e95f6898cf179
-- compiler key: cc7a8093a2fc454e407e504b031e3bf381b1d4004d6bc8b76e9e95f6898cf179 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.sim_orders as base
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
