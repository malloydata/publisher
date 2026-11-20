-- hammer/18-parameter-eligibility/f2b/f2b.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source high_value_daily
-- sourceID: high_value_daily@<hammer>/f2b/f2b.malloy
-- connection: orders_pg
-- sourceEntityId: 2366a624d0cf11ca69e0a782e347b8963aacba4a638baa46c8260dc49a8beb02
-- compiler key: 2366a624d0cf11ca69e0a782e347b8963aacba4a638baa46c8260dc49a8beb02 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.f2b_orders as base
WHERE base."amount">=100
GROUP BY 1
ORDER BY 1 desc NULLS LAST
