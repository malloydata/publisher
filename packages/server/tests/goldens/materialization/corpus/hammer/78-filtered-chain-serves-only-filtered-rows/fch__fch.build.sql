-- hammer/78-filtered-chain-serves-only-filtered-rows/fch/fch.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source deals
-- sourceID: deals@<hammer>/fch/fch.malloy
-- connection: orders_pg
-- sourceEntityId: 0661ea6f1ff1c4b38c274b4742f96bac1ebcf1b6b2ef0dfe34c114274b6ff586
-- compiler key: 0661ea6f1ff1c4b38c274b4742f96bac1ebcf1b6b2ef0dfe34c114274b6ff586 (the sourceEntityId)
-- build SQL:
SELECT deal_id, amount, is_deleted, is_open FROM public.fch_deals

-- ==== source pipeline_now
-- sourceID: pipeline_now@<hammer>/fch/fch.malloy
-- connection: orders_pg
-- sourceEntityId: 0661ea6f1ff1c4b38c274b4742f96bac1ebcf1b6b2ef0dfe34c114274b6ff586
-- compiler key: 0661ea6f1ff1c4b38c274b4742f96bac1ebcf1b6b2ef0dfe34c114274b6ff586 (the sourceEntityId)
-- build SQL:
SELECT deal_id, amount, is_deleted, is_open FROM public.fch_deals
