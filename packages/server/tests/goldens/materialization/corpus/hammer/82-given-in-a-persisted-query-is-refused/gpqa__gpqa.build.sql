-- hammer/82-given-in-a-persisted-query-is-refused/gpqa/gpqa.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source arg_bound
-- sourceID: arg_bound@<hammer>/gpqa/gpqa.malloy
-- connection: orders_pg
-- sourceEntityId: d63dbb37e658d4db170091453c4eab45265ccfed2a4b74151c6bea2acb677d16
-- compiler key: d63dbb37e658d4db170091453c4eab45265ccfed2a4b74151c6bea2acb677d16 (the sourceEntityId)
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."org_id" as "org_id"
FROM (SELECT org_id, amount FROM public.gpq_rows) as base
WHERE base."org_id"=1
