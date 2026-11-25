-- hammer/10-a-lookback-re-reads-a-window/ivk/ivk.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source events
-- sourceID: events@<hammer>/ivk/ivk.malloy
-- connection: orders_pg
-- sourceEntityId: 00d5a52424c0fa84173b7772dff51f6ea77ea3ed9a9b291a0b223199379256ff
-- compiler key: 00d5a52424c0fa84173b7772dff51f6ea77ea3ed9a9b291a0b223199379256ff (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."amount" as "amount",
   base."loaded_at" as "loaded_at"
FROM public.ivk_events as base
