-- hammer/27-concurrent-build-conflict/cbc/cbc.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/cbc/cbc.malloy
-- connection: orders_pg
-- sourceEntityId: d5c5e31419d48ef5cffcad7a3d8deb0ba643457e57ded41ef7ac6650f7340c4d
-- compiler key: d5c5e31419d48ef5cffcad7a3d8deb0ba643457e57ded41ef7ac6650f7340c4d (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cbc_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
