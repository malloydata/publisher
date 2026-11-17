-- hammer/23-data-type-roundtrip/dt/dt.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source all_rows
-- sourceID: all_rows@<hammer>/dt/dt.malloy
-- connection: orders_pg
-- sourceEntityId: 0ccb468450fecdc2757aaa1cb779eb3180105131ee5266496fdca0ce066db65a
-- compiler key: 0ccb468450fecdc2757aaa1cb779eb3180105131ee5266496fdca0ce066db65a (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."d" as "d",
   base."amt" as "amt",
   base."flag" as "flag",
   base."label" as "label"
FROM public.dt_rows as base
ORDER BY 1 ASC NULLS LAST
