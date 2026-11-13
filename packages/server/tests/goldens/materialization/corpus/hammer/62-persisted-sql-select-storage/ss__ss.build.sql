-- hammer/62-persisted-sql-select-storage/ss/ss.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source raw
-- sourceID: raw@<hammer>/ss/ss.malloy
-- connection: orders_pg
-- sourceEntityId: ea1ce5a0a405bd0258bb8f4191a887ac9d95c9bd52c9c165052fe3831d0f6927
-- compiler key: ea1ce5a0a405bd0258bb8f4191a887ac9d95c9bd52c9c165052fe3831d0f6927 (the sourceEntityId)
-- build SQL:
SELECT order_date, amount FROM public.ss_orders
