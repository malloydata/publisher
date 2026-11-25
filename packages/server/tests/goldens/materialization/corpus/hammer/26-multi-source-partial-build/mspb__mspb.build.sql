-- hammer/26-multi-source-partial-build/mspb/mspb.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source by_date
-- sourceID: by_date@<hammer>/mspb/mspb.malloy
-- connection: orders_pg
-- sourceEntityId: d6c37c29fc8baee87b8667aba9d2fe320ea110412a4194593beccb99eed305ab
-- compiler key: d6c37c29fc8baee87b8667aba9d2fe320ea110412a4194593beccb99eed305ab (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.mspb_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source by_region
-- sourceID: by_region@<hammer>/mspb/mspb.malloy
-- connection: orders_pg
-- sourceEntityId: 31ac7d7e8c691c6f853b19046ea61497134e547f19351399492dabd7fe693df7
-- compiler key: 31ac7d7e8c691c6f853b19046ea61497134e547f19351399492dabd7fe693df7 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.mspb_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
