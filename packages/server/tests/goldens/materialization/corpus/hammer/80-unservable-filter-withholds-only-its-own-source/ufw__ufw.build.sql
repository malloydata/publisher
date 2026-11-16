-- hammer/80-unservable-filter-withholds-only-its-own-source/ufw/ufw.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source big_orders
-- sourceID: big_orders@<hammer>/ufw/ufw.malloy
-- connection: orders_pg
-- sourceEntityId: b3a7b4387305e65a36cc06104d98177215fbab151e1c3a6efdff636e75d03094
-- compiler key: ce93f068a04f2bc1d492bc4e4076e4254c853603a798d714265a7f93370a6b08
-- build SQL:
SELECT order_id, amount, region_id FROM public.ufw_orders

-- ==== source north_orders
-- sourceID: north_orders@<hammer>/ufw/ufw.malloy
-- connection: orders_pg
-- sourceEntityId: 44589c74b782738c623f171e7e7c544148c90a3890ccf366d4970854621b76ac
-- compiler key: ce93f068a04f2bc1d492bc4e4076e4254c853603a798d714265a7f93370a6b08
-- build SQL:
SELECT order_id, amount, region_id FROM public.ufw_orders
