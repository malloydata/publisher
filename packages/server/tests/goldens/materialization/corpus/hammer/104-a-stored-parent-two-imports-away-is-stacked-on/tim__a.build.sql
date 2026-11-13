-- hammer/104-a-stored-parent-two-imports-away-is-stacked-on/tim/a.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/tim/a.malloy
-- connection: orders_pg
-- sourceEntityId: 77d1ee2d60220500033e17b15b7fe81dd68320356a8d460ec224f14100d5b3a6
-- compiler key: 77d1ee2d60220500033e17b15b7fe81dd68320356a8d460ec224f14100d5b3a6 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.tim_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
