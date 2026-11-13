-- hammer/82-given-in-a-persisted-query-is-refused/gpqb/gpqb.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source outside
-- sourceID: outside@<hammer>/gpqb/gpqb.malloy
-- connection: orders_pg
-- sourceEntityId: 942f2859572fa2acb5ad81db6a9b1b5b23d38353818b53407289545b4e2fe3fc
-- compiler key: 942f2859572fa2acb5ad81db6a9b1b5b23d38353818b53407289545b4e2fe3fc (the sourceEntityId)
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."org_id" as "org_id"
FROM (SELECT org_id, amount FROM public.gpq_rows) as base
