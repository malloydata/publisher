-- hammer/97-an-ungrouped-aggregate-on-a-lifted-source-serves-from-storage/uag/uag.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/uag/uag.malloy
-- connection: orders_pg
-- sourceEntityId: 0b24a07d9e9b13815d748b6717a3aa1477876823bbe8f801afa6f706e6a9e233
-- compiler key: 0b24a07d9e9b13815d748b6717a3aa1477876823bbe8f801afa6f706e6a9e233 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.uag_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source shares
-- sourceID: shares@<hammer>/uag/uag.malloy
-- connection: orders_pg
-- sourceEntityId: 0b24a07d9e9b13815d748b6717a3aa1477876823bbe8f801afa6f706e6a9e233
-- compiler key: 0b24a07d9e9b13815d748b6717a3aa1477876823bbe8f801afa6f706e6a9e233 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.uag_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
