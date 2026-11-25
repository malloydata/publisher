-- hammer/111-a-source-reading-its-destinations-own-catalog-builds-into-it/scbsql/scbsql.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source by_kind_sql
-- sourceID: by_kind_sql@<hammer>/scbsql/scbsql.malloy
-- connection: lake_probe
-- sourceEntityId: 4a0727e998edd72f86d3290aee2bd8f59aae3fbc842699b66bf9b1c839386a35
-- compiler key: 4a0727e998edd72f86d3290aee2bd8f59aae3fbc842699b66bf9b1c839386a35 (the sourceEntityId)
-- build SQL:
SELECT 
   base."kind" as "kind"
FROM (SELECT kind FROM lake_probe.main.scb_events) as base
GROUP BY 1
ORDER BY 1 asc NULLS LAST
