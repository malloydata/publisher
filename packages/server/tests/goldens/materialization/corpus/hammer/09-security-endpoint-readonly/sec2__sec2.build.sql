-- hammer/09-security-endpoint-readonly/sec2/sec2.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/sec2/sec2.malloy
-- connection: orders_pg
-- sourceEntityId: 2d06f55062c36217892c7a897c215299d572239dda395f53a8be724d9ee687aa
-- compiler key: 2d06f55062c36217892c7a897c215299d572239dda395f53a8be724d9ee687aa (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.sec2_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
