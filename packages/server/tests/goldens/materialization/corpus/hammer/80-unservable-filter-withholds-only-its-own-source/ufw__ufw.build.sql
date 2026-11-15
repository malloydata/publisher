-- hammer/80-unservable-filter-withholds-only-its-own-source/ufw/ufw.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source big_orders
-- sourceID: big_orders@<hammer>/ufw/ufw.malloy
-- connection: orders_pg
-- sourceEntityId: ce93f068a04f2bc1d492bc4e4076e4254c853603a798d714265a7f93370a6b08
-- compiler key: ce93f068a04f2bc1d492bc4e4076e4254c853603a798d714265a7f93370a6b08 (the sourceEntityId)
-- build SQL:
SELECT order_id, amount, region_id FROM public.ufw_orders

-- ==== source north_orders
-- sourceID: north_orders@<hammer>/ufw/ufw.malloy
-- connection: orders_pg
-- sourceEntityId: ce93f068a04f2bc1d492bc4e4076e4254c853603a798d714265a7f93370a6b08
-- compiler key: ce93f068a04f2bc1d492bc4e4076e4254c853603a798d714265a7f93370a6b08 (the sourceEntityId)
-- build SQL:
SELECT order_id, amount, region_id FROM public.ufw_orders
