-- hammer/05-a-stored-source-built-on-another-rebuilds/ivc/ivc.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source batch_totals
-- sourceID: batch_totals@<hammer>/ivc/ivc.malloy
-- connection: orders_pg
-- sourceEntityId: 98a9b66e913464c7653ea2b213b70a5e0b49a6224e5820587102347c6fd459be
-- compiler key: 98a9b66e913464c7653ea2b213b70a5e0b49a6224e5820587102347c6fd459be (the sourceEntityId)
-- build SQL:
SELECT 
   base."batch" as "batch",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.ivc_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source doubled
-- sourceID: doubled@<hammer>/ivc/ivc.malloy
-- connection: orders_pg
-- reads: batch_totals@<hammer>/ivc/ivc.malloy
-- sourceEntityId: 9d7e97a0b40f210d3911745033de9529cf5282b33f3eb0f9159568015c305bdf
-- compiler key: 9d7e97a0b40f210d3911745033de9529cf5282b33f3eb0f9159568015c305bdf (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."batch" as "batch",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM public.ivc_orders as base
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   base."batch" as "batch",
   COALESCE(SUM(base."total_amount"),0)*2 as "doubled_total"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   base."batch" as "batch",
   COALESCE(SUM(base."total_amount"),0)*2 as "doubled_total"
FROM built.batch_totals as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
