-- hammer/15-join-across-two-persisted/jt/jt.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source regions
-- sourceID: regions@<hammer>/jt/jt.malloy
-- connection: orders_pg
-- sourceEntityId: 9c2f05a717fe2161bbb147f590d625da5de3a7bc7a8b4c0d4e5cffa38b83982f
-- compiler key: 9c2f05a717fe2161bbb147f590d625da5de3a7bc7a8b4c0d4e5cffa38b83982f (the sourceEntityId)
-- build SQL:
SELECT 
   base."region_id" as "region_id",
   base."region_name" as "region_name"
FROM public.jt_regions as base
GROUP BY 1,2
ORDER BY 1 asc NULLS LAST

-- ==== source orders
-- sourceID: orders@<hammer>/jt/jt.malloy
-- connection: orders_pg
-- reads: regions@<hammer>/jt/jt.malloy
-- sourceEntityId: 564149a103dc700271633fbe82a9b0b1762021a99d6dfda750206ff461bd12e8
-- compiler key: 564149a103dc700271633fbe82a9b0b1762021a99d6dfda750206ff461bd12e8 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."amount" as "amount",
   base."region_id" as "region_id"
FROM public.jt_orders as base
GROUP BY 1,2,3
ORDER BY 1 asc NULLS LAST
