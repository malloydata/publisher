-- hammer/76-filtered-source-serves-only-filtered-rows/fsf/fsf.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source high_value
-- sourceID: high_value@<hammer>/fsf/fsf.malloy
-- connection: orders_pg
-- sourceEntityId: a857cf0513a16b2d1083c6517064006c7128c790446b80d301d9d2bac7eb8fcd
-- compiler key: a857cf0513a16b2d1083c6517064006c7128c790446b80d301d9d2bac7eb8fcd (the sourceEntityId)
-- build SQL:
SELECT order_id, amount FROM public.fsf_orders
