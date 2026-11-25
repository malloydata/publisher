-- hammer/111-a-source-reading-its-destinations-own-catalog-builds-into-it/scb/scb.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source by_kind
-- sourceID: by_kind@<hammer>/scb/scb.malloy
-- connection: lake_probe
-- sourceEntityId: 6114093e2543f407a900fc53c472d8a2c6f09ebf2d85a9bc7151a6ae786ec241
-- compiler key: 6114093e2543f407a900fc53c472d8a2c6f09ebf2d85a9bc7151a6ae786ec241 (the sourceEntityId)
-- build SQL:
SELECT 
   base."kind" as "kind",
   COUNT(1) as "n"
FROM lake_probe.main.scb_events as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
