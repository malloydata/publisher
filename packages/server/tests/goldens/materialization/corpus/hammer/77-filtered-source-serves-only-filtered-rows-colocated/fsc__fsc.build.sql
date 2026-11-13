-- hammer/77-filtered-source-serves-only-filtered-rows-colocated/fsc/fsc.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source high_value
-- sourceID: high_value@<hammer>/fsc/fsc.malloy
-- connection: orders_pg
-- sourceEntityId: 3a00145107d56d0efac2c2b9848ad4d707d854074c6ed879aa92ea34cd113ba8
-- compiler key: 3a00145107d56d0efac2c2b9848ad4d707d854074c6ed879aa92ea34cd113ba8 (the sourceEntityId)
-- build SQL:
SELECT order_id, amount FROM public.fsc_orders
