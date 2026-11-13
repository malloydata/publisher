-- hammer/46-duckdb-source-not-materializable/dk/dk.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source dk_daily
-- sourceID: dk_daily@<hammer>/dk/dk.malloy
-- connection: dk
-- sourceEntityId: 5eafafa08ff015626e724d9ea84360e554828288845cf50f0b6dac000f6a2e60
-- compiler key: 5eafafa08ff015626e724d9ea84360e554828288845cf50f0b6dac000f6a2e60 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM (
  SELECT 'US' AS region, 100 AS amount
  UNION ALL SELECT 'EU', 200
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
