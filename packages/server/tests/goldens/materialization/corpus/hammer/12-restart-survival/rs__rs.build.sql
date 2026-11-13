-- hammer/12-restart-survival/rs/rs.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/rs/rs.malloy
-- connection: orders_pg
-- sourceEntityId: 27ac4345d6438331d67d2a7d43e684c666b0fe2015fff3da07f65849239b36b3
-- compiler key: 27ac4345d6438331d67d2a7d43e684c666b0fe2015fff3da07f65849239b36b3 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.rs_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
