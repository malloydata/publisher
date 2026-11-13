-- hammer/35-cross-environment-same-name/cesame/ce.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cesame/ce.malloy
-- connection: orders_pg
-- sourceEntityId: 713c86582c6aa8cca575b1d62fd3f3ad44a26eb599a356a086a7388df5501792
-- compiler key: 713c86582c6aa8cca575b1d62fd3f3ad44a26eb599a356a086a7388df5501792 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.ce_orders_e1 as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
