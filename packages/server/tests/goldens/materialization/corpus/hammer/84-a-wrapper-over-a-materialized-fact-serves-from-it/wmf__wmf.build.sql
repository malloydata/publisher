-- hammer/84-a-wrapper-over-a-materialized-fact-serves-from-it/wmf/wmf.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source _orders_fact
-- sourceID: _orders_fact@<hammer>/wmf/wmf.malloy
-- connection: orders_pg
-- sourceEntityId: f53e38ebc79dae53078691595c194b4e45f21c3a6a8284b4163b76abf28242c6
-- compiler key: f53e38ebc79dae53078691595c194b4e45f21c3a6a8284b4163b76abf28242c6 (the sourceEntityId)
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."order_id" as "order_id",
   base."org_id" as "org_id",
   base."region_id" as "region_id"
FROM (SELECT order_id, org_id, region_id, amount FROM public.wmf_orders) as base
