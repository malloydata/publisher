-- hammer/127-a-database-name-in-a-missing-schema-is-refused-at-plan/cnf/cnf.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily_orders
-- sourceID: daily_orders@<hammer>/cnf/cnf.malloy
-- connection: orders_pg
-- sourceEntityId: 9cb30638c651ed189bf171b3f4b67167883f77825c68f7c31e6ef1ea08541886
-- compiler key: 9cb30638c651ed189bf171b3f4b67167883f77825c68f7c31e6ef1ea08541886 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.cnf_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
