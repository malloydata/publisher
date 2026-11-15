-- hammer/06-a-query-term-over-materialized-sources-still-scopes/qts/qts.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders_all
-- sourceID: orders_all@<hammer>/qts/qts.malloy
-- connection: orders_pg
-- sourceEntityId: 8253ea9dafc65cea4d366d6750be5bc5d40d39689777ec8959cadf06bbe00742
-- compiler key: cc23d682c888b6f522ba261b7951ef52145a1b30b3f1b532ac237c9b5bed7777
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."list_id" as "list_id",
   base."order_id" as "order_id",
   base."org_id" as "org_id"
FROM (SELECT order_id, org_id, list_id, amount FROM public.qts_orders) as base

-- ==== source visible_orders
-- sourceID: visible_orders@<hammer>/qts/qts.malloy
-- connection: orders_pg
-- sourceEntityId: 8253ea9dafc65cea4d366d6750be5bc5d40d39689777ec8959cadf06bbe00742
-- compiler key: cc23d682c888b6f522ba261b7951ef52145a1b30b3f1b532ac237c9b5bed7777
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."list_id" as "list_id",
   base."order_id" as "order_id",
   base."org_id" as "org_id"
FROM (SELECT order_id, org_id, list_id, amount FROM public.qts_orders) as base

-- ==== source vis_all
-- sourceID: vis_all@<hammer>/qts/qts.malloy
-- connection: orders_pg
-- sourceEntityId: 6cdf335f7dd739a6a50a98655f3c0f7e903421dbe38466d384af4d9a829641be
-- compiler key: 078aa0b9c4af42d0dbfe0de503073ec707e9dfd2ebd4b9d37772740cee5ae011
-- build SQL:
SELECT 
   base."list_id" as "list_id",
   base."org_id" as "org_id",
   base."user_id" as "user_id"
FROM (SELECT org_id, user_id, list_id FROM public.qts_visibility) as base
