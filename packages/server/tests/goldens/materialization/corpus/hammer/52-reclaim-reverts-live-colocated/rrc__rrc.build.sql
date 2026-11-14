-- hammer/52-reclaim-reverts-live-colocated/rrc/rrc.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/rrc/rrc.malloy
-- connection: orders_pg
-- sourceEntityId: 15b55e6dc57f590c3eadf4c175108973723ce5e4771a5d3d8a725c7f9d23a7dd
-- compiler key: 15b55e6dc57f590c3eadf4c175108973723ce5e4771a5d3d8a725c7f9d23a7dd (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.rrc_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
