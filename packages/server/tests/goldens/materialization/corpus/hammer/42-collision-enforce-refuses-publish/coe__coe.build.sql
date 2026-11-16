-- hammer/42-collision-enforce-refuses-publish/coe/coe.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source alpha
-- sourceID: alpha@<hammer>/coe/coe.malloy
-- connection: orders_pg
-- sourceEntityId: fc620d71b142acae673f709f11c58c4435e62b6bec1deef8c57d21d48491ed74
-- compiler key: fc620d71b142acae673f709f11c58c4435e62b6bec1deef8c57d21d48491ed74 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "n"
FROM public.coe_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source beta
-- sourceID: beta@<hammer>/coe/coe.malloy
-- connection: orders_pg
-- sourceEntityId: 811aae00abcb99687d8bd1f64915a40f2a92349ebde02f851d48ba81058ede84
-- compiler key: 811aae00abcb99687d8bd1f64915a40f2a92349ebde02f851d48ba81058ede84 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.coe_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
