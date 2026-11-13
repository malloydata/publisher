-- hammer/73-off-must-not-build-into-the-warehouse/osw/osw.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/osw/osw.malloy
-- connection: orders_pg
-- sourceEntityId: 0509c30bc8e5f7b7a855418fa8316f09369a31e01f81197b5e1a425f9ff0b5fb
-- compiler key: 0509c30bc8e5f7b7a855418fa8316f09369a31e01f81197b5e1a425f9ff0b5fb (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.osw_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
