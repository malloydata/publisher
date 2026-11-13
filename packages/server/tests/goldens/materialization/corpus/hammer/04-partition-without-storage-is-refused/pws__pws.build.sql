-- hammer/04-partition-without-storage-is-refused/pws/pws.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders
-- sourceID: orders@<hammer>/pws/pws.malloy
-- connection: orders_pg
-- sourceEntityId: 07a8f5115ffc9c3654abb7f84215819edba831cde166131328a61361a5897feb
-- compiler key: 07a8f5115ffc9c3654abb7f84215819edba831cde166131328a61361a5897feb (the sourceEntityId)
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."order_id" as "order_id",
   base."org_id" as "org_id"
FROM (SELECT order_id, org_id, amount FROM public.pws_orders) as base
