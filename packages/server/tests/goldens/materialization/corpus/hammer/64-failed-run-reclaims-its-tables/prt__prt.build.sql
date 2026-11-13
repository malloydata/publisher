-- hammer/64-failed-run-reclaims-its-tables/prt/prt.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source a
-- sourceID: a@<hammer>/prt/prt.malloy
-- connection: orders_pg
-- sourceEntityId: ef59ef604cf0537e4feb3033b52a659cc58b225653ea5d6099bcba3c226073a9
-- compiler key: ef59ef604cf0537e4feb3033b52a659cc58b225653ea5d6099bcba3c226073a9 (the sourceEntityId)
-- build SQL:
SELECT 
   base."k" as "k",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.prt_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source b
-- sourceID: b@<hammer>/prt/prt.malloy
-- connection: orders_pg
-- reads: a@<hammer>/prt/prt.malloy
-- sourceEntityId: ba0a6a4fc6cc47b06d9a4c4f8e5b1f1c1012b5c693a95b860fdf8cb33b2c0c70
-- compiler key: ba0a6a4fc6cc47b06d9a4c4f8e5b1f1c1012b5c693a95b860fdf8cb33b2c0c70 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."k" as "k",
     COALESCE(SUM(base."amount"),0) as "total"
  FROM public.prt_orders as base
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   COALESCE(SUM(base."total"),0) as "grand"
FROM __stage0 as base
-- build SQL with every other planned source built:
SELECT 
   COALESCE(SUM(base."total"),0) as "grand"
FROM built.a as base
