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
-- sourceEntityId: 20cbf8a728c69cdcdc50194c7da4b5e36de9e9fbaecdf74e229d32927e58c604
-- compiler key: 20cbf8a728c69cdcdc50194c7da4b5e36de9e9fbaecdf74e229d32927e58c604 (the sourceEntityId)
-- build SQL:
SELECT order_id, amount, region_id FROM public.fmj_orders

-- ==== source broken_orders
-- sourceID: broken_orders@<hammer>/fmj/fmj.malloy
-- connection: orders_pg
-- sourceEntityId: 20cbf8a728c69cdcdc50194c7da4b5e36de9e9fbaecdf74e229d32927e58c604
-- compiler key: 20cbf8a728c69cdcdc50194c7da4b5e36de9e9fbaecdf74e229d32927e58c604 (the sourceEntityId)
-- build SQL:
SELECT order_id, amount, region_id FROM public.fmj_orders

-- ==== source big_orders
-- sourceID: big_orders@<hammer>/fmj/fmj.malloy
-- connection: orders_pg
-- sourceEntityId: 20cbf8a728c69cdcdc50194c7da4b5e36de9e9fbaecdf74e229d32927e58c604
-- compiler key: 20cbf8a728c69cdcdc50194c7da4b5e36de9e9fbaecdf74e229d32927e58c604 (the sourceEntityId)
-- build SQL:
SELECT order_id, amount, region_id FROM public.fmj_orders
