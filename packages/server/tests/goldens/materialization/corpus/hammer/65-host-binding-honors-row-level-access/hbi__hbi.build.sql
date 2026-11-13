-- hammer/65-host-binding-honors-row-level-access/hbi/hbi.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source all_rollup
-- sourceID: all_rollup@<hammer>/hbi/hbi.malloy
-- connection: orders_pg
-- sourceEntityId: 50e08c877886851d137cfa81f0c95d5dd2735057201536a5d54273b0847ec264
-- compiler key: 50e08c877886851d137cfa81f0c95d5dd2735057201536a5d54273b0847ec264 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "t"
FROM public.hbi_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source scoped_rollup
-- sourceID: scoped_rollup@<hammer>/hbi/hbi.malloy
-- connection: orders_pg
-- sourceEntityId: b1c4ae28e6520a507f4ffbfabb12725bfa8c3550e843b3026ec8853301598994
-- compiler key: b1c4ae28e6520a507f4ffbfabb12725bfa8c3550e843b3026ec8853301598994 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "t"
FROM public.hbi_orders as base
WHERE base."region" = 'US'
GROUP BY 1
ORDER BY 2 desc NULLS LAST
