-- hammer/67-host-binding-of-unplanned-source/hbd/hbd.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source all_rows
-- sourceID: all_rows@<hammer>/hbd/hbd.malloy
-- connection: orders_pg
-- sourceEntityId: e4602c5b0af31c1f245edd939382d839251e6668e80a37b4896b696d05eb0451
-- compiler key: e4602c5b0af31c1f245edd939382d839251e6668e80a37b4896b696d05eb0451 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."region" as "region",
   base."amount" as "amount"
FROM public.hbd_orders as base
