-- hammer/08-security-user-sql-cannot-mutate-lake/sec1/sec1.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/sec1/sec1.malloy
-- connection: orders_pg
-- sourceEntityId: 6aaac23569f8058111429d25d4eb8fd6adb20c3e1594c1f37c56d4846ab98e5c
-- compiler key: 6aaac23569f8058111429d25d4eb8fd6adb20c3e1594c1f37c56d4846ab98e5c (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.sec1_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
