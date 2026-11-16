-- hammer/29-package-unload-while-serving/puw/puw.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/puw/puw.malloy
-- connection: orders_pg
-- sourceEntityId: 09b129311442b1a0314930cc3c983e2bfd869810e1a46808c09391a85abc01a3
-- compiler key: 09b129311442b1a0314930cc3c983e2bfd869810e1a46808c09391a85abc01a3 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.puw_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
