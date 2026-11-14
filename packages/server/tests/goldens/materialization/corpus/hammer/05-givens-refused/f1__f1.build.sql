-- hammer/05-givens-refused/f1/f1.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source scoped_rollup
-- sourceID: scoped_rollup@<hammer>/f1/f1.malloy
-- connection: orders_pg
-- sourceEntityId: 3a614fdc27fe0a38abc4556d4510968dac3af2f0f6a08dee61466554b92e3ab8
-- compiler key: 3a614fdc27fe0a38abc4556d4510968dac3af2f0f6a08dee61466554b92e3ab8 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "t"
FROM public.f1_orders as base
WHERE true
GROUP BY 1
ORDER BY 1 desc NULLS LAST
