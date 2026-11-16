-- hammer/121-storage-never-falls-back-to-the-warehouse/nw/nw.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/nw/nw.malloy
-- connection: orders_pg
-- sourceEntityId: d0f7f6e253375aa7c5e4ceff9c247d56bf9dfa175c6985eeb28525384f18170a
-- compiler key: d0f7f6e253375aa7c5e4ceff9c247d56bf9dfa175c6985eeb28525384f18170a (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.nw_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source colocated_daily
-- sourceID: colocated_daily@<hammer>/nw/nw.malloy
-- connection: orders_pg
-- sourceEntityId: de1fb34ea9ae01a3dc662c160fd9d1d3d49299db910cc3550c817fd8a31f51f1
-- compiler key: de1fb34ea9ae01a3dc662c160fd9d1d3d49299db910cc3550c817fd8a31f51f1 (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   COUNT(1) as "n"
FROM public.nw_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
