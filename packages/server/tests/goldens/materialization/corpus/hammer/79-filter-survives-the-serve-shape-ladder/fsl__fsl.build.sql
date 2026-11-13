-- hammer/79-filter-survives-the-serve-shape-ladder/fsl/fsl.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders
-- sourceID: orders@<hammer>/fsl/fsl.malloy
-- connection: orders_pg
-- sourceEntityId: 2159477a020fce53d51a2efc1d30adddc219504dfcec0dfd194a2121810995b4
-- compiler key: 2159477a020fce53d51a2efc1d30adddc219504dfcec0dfd194a2121810995b4 (the sourceEntityId)
-- build SQL:
SELECT order_id, amount, region_id FROM public.fsl_orders
