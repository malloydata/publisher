-- hammer/70-strict-upstream-built-here/sbh/sbh.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/sbh/sbh.malloy
-- connection: orders_pg
-- sourceEntityId: 7c769a2988ad1793356217858a2327168a9e194d6910cc598174fc4ce0273b93
-- compiler key: 7c769a2988ad1793356217858a2327168a9e194d6910cc598174fc4ce0273b93 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.sbh_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source rollup
-- sourceID: rollup@<hammer>/sbh/sbh.malloy
-- connection: orders_pg
-- reads: daily@<hammer>/sbh/sbh.malloy
-- sourceEntityId: c632e21d38920afa8a6eca2dfc9c690dd369087a28e1c7d670f80bb4b1a33148
-- compiler key: c632e21d38920afa8a6eca2dfc9c690dd369087a28e1c7d670f80bb4b1a33148 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."order_date" as "order_date",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.sbh_orders as base
  GROUP BY 1
  ORDER BY 1 desc NULLS LAST
)
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM __stage0 as base
-- build SQL with every other planned source built:
SELECT 
   COALESCE(SUM(base."total_amount"),0) as "grand_total"
FROM built.daily as base
