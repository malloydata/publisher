-- hammer/05-a-merge-key-must-not-match-rows-outside-the-callers-scope/xt/xt.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source order_rows
-- sourceID: order_rows@<hammer>/xt/xt.malloy
-- connection: orders_pg
-- sourceEntityId: a5362d1c70d80dd3e761f0cd761d1175187b5f2645fc76c5963d2b4202d554ba
-- compiler key: a5362d1c70d80dd3e761f0cd761d1175187b5f2645fc76c5963d2b4202d554ba (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."org_id" as "org_id",
   base."batch" as "batch",
   COALESCE(SUM(base."amount"),0) as "amount_total"
FROM public.xt_orders as base
GROUP BY 1,2,3
ORDER BY 4 desc NULLS LAST
