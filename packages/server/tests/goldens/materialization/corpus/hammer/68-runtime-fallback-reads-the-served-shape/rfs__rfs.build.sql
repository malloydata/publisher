-- hammer/68-runtime-fallback-reads-the-served-shape/rfs/rfs.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/rfs/rfs.malloy
-- connection: orders_pg
-- sourceEntityId: 9e5e51436de410d2744df57f04a2f660aaebe9725a0d8bd9449f763a168c3f50
-- compiler key: 9e5e51436de410d2744df57f04a2f660aaebe9725a0d8bd9449f763a168c3f50 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.rfs_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source weekly
-- sourceID: weekly@<hammer>/rfs/rfs.malloy
-- connection: orders_pg
-- sourceEntityId: 5b1498387954654e207f8792e17e1bda2336913eda8007641eb0a025a713db6e
-- compiler key: 5b1498387954654e207f8792e17e1bda2336913eda8007641eb0a025a713db6e (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "orders_count"
FROM public.rfs_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
