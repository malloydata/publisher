-- hammer/115-a-mixed-case-or-reserved-persist-name-needs-no-quoting/pn/pn.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source mixed
-- sourceID: mixed@<hammer>/pn/pn.malloy
-- connection: orders_pg
-- sourceEntityId: 2bf93368f93ec56cbf5330260968f708d32d8358e1481cc85525f1bf3cfc64da
-- compiler key: 2bf93368f93ec56cbf5330260968f708d32d8358e1481cc85525f1bf3cfc64da (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.pn_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source reserved
-- sourceID: reserved@<hammer>/pn/pn.malloy
-- connection: orders_pg
-- sourceEntityId: a8e819d69e4003aeabb8a41ba2bd20188a4120cd183a0115b867e51bec4e7d6a
-- compiler key: a8e819d69e4003aeabb8a41ba2bd20188a4120cd183a0115b867e51bec4e7d6a (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total",
   COUNT(1) as "n"
FROM public.pn_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
