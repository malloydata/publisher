-- hammer/59-opt-out-persist-recomputes/nop/nop.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/nop/nop.malloy
-- connection: orders_pg
-- sourceEntityId: 702384af7c548f120a4fa7c280e7c82cd05b7ac2d1cf70e98a44b9dc0904452c
-- compiler key: 702384af7c548f120a4fa7c280e7c82cd05b7ac2d1cf70e98a44b9dc0904452c (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.nop_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
