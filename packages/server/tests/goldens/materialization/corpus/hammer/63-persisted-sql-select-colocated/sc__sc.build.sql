-- hammer/63-persisted-sql-select-colocated/sc/sc.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source raw
-- sourceID: raw@<hammer>/sc/sc.malloy
-- connection: orders_pg
-- sourceEntityId: cd0a260337a2b968dead4b34503d642b77c6be6882ea97807ca5db4a6bb3cee4
-- compiler key: cd0a260337a2b968dead4b34503d642b77c6be6882ea97807ca5db4a6bb3cee4 (the sourceEntityId)
-- build SQL:
SELECT order_date, amount FROM public.sc_orders
