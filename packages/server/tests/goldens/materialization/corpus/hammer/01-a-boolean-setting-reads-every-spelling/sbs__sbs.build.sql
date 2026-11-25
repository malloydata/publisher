-- hammer/01-a-boolean-setting-reads-every-spelling/sbs/sbs.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source alpha
-- sourceID: alpha@<hammer>/sbs/sbs.malloy
-- connection: orders_pg
-- sourceEntityId: 42fb492cb3a0b0328859d9f43465ab0dc385bae580cf1258fdf92841341ac7e2
-- compiler key: 42fb492cb3a0b0328859d9f43465ab0dc385bae580cf1258fdf92841341ac7e2 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "n"
FROM public.sbs_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source beta
-- sourceID: beta@<hammer>/sbs/sbs.malloy
-- connection: orders_pg
-- sourceEntityId: c5d73e0a2f0dc61461b7331162dc73efa42862dab5d46874880886b7ac08353a
-- compiler key: c5d73e0a2f0dc61461b7331162dc73efa42862dab5d46874880886b7ac08353a (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.sbs_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
