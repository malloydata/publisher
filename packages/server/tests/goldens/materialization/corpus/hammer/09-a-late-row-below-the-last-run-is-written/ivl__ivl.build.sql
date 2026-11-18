-- hammer/09-a-late-row-below-the-last-run-is-written/ivl/ivl.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source events
-- sourceID: events@<hammer>/ivl/ivl.malloy
-- connection: orders_pg
-- sourceEntityId: 5f853126c98e0bbbf7adcd993bc8d647ba38b72889f448af72cdc21a79ef89fb
-- compiler key: 5f853126c98e0bbbf7adcd993bc8d647ba38b72889f448af72cdc21a79ef89fb (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."amount" as "amount",
   base."loaded_at" as "loaded_at"
FROM public.ivl_events as base
