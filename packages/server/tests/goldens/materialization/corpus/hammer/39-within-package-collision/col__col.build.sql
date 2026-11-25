-- hammer/39-within-package-collision/col/col.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source alpha
-- sourceID: alpha@<hammer>/col/col.malloy
-- connection: orders_pg
-- sourceEntityId: 5505bfa86d8c9f25c8acd06c20a0931446a33a11629597f6168699002eaff5f7
-- compiler key: 5505bfa86d8c9f25c8acd06c20a0931446a33a11629597f6168699002eaff5f7 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "n"
FROM public.col_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source beta
-- sourceID: beta@<hammer>/col/col.malloy
-- connection: orders_pg
-- sourceEntityId: eeb4c6ada4d7c933abb4b997bc3e91c9a9965e3b9d2faaa46459373027038d07
-- compiler key: eeb4c6ada4d7c933abb4b997bc3e91c9a9965e3b9d2faaa46459373027038d07 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.col_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
