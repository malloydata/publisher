-- hammer/81-filter-through-a-materialized-join-still-filters/fmj/fmj.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source regions
-- sourceID: regions@<hammer>/fmj/fmj.malloy
-- connection: orders_pg
-- sourceEntityId: 41bb2149613988fd939c3f76d037a9ec4589048c3c09a8114c553f10828b2e3e
-- compiler key: 41bb2149613988fd939c3f76d037a9ec4589048c3c09a8114c553f10828b2e3e (the sourceEntityId)
-- build SQL:
SELECT region_id, region_name FROM public.fmj_regions

-- ==== source north_orders
-- sourceID: north_orders@<hammer>/fmj/fmj.malloy
-- connection: orders_pg
-- sourceEntityId: c316755ea23a8572000cf35157fcef1e7ec66531a8ad3effc91072d515674fca
-- compiler key: 20cbf8a728c69cdcdc50194c7da4b5e36de9e9fbaecdf74e229d32927e58c604
-- build SQL:
SELECT order_id, amount, region_id FROM public.fmj_orders

-- ==== source broken_orders
-- sourceID: broken_orders@<hammer>/fmj/fmj.malloy
-- connection: orders_pg
-- sourceEntityId: f876cebac232b0d36368548a60e71e8500d2c75ed9e1dd0da56eed67b1aac898
-- compiler key: 20cbf8a728c69cdcdc50194c7da4b5e36de9e9fbaecdf74e229d32927e58c604
-- build SQL:
SELECT order_id, amount, region_id FROM public.fmj_orders

-- ==== source big_orders
-- sourceID: big_orders@<hammer>/fmj/fmj.malloy
-- connection: orders_pg
-- sourceEntityId: 12947a8b49fa9f5f2081b0d2dcc7a398a3eb800823bf6f8e4494f89340a88e74
-- compiler key: 20cbf8a728c69cdcdc50194c7da4b5e36de9e9fbaecdf74e229d32927e58c604
-- build SQL:
SELECT order_id, amount, region_id FROM public.fmj_orders
