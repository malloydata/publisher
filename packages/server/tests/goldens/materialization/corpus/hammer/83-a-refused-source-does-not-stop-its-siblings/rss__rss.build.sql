-- hammer/83-a-refused-source-does-not-stop-its-siblings/rss/rss.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source by_date
-- sourceID: by_date@<hammer>/rss/rss.malloy
-- connection: orders_pg
-- sourceEntityId: d6ce0dba9f8d006ca13abcbcf57fa6f08b5d02dc1dfcf2a7b8a1914d7ae5f7f2
-- compiler key: d6ce0dba9f8d006ca13abcbcf57fa6f08b5d02dc1dfcf2a7b8a1914d7ae5f7f2 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.rss_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source scoped_rollup
-- sourceID: scoped_rollup@<hammer>/rss/rss.malloy
-- connection: orders_pg
-- sourceEntityId: dd21848c2eb4f792465ce5d7dec8b45ec54dcac0be7f917993d28f5c5557310b
-- compiler key: dd21848c2eb4f792465ce5d7dec8b45ec54dcac0be7f917993d28f5c5557310b (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "t"
FROM public.rss_orders as base
WHERE true
GROUP BY 1
ORDER BY 1 desc NULLS LAST
