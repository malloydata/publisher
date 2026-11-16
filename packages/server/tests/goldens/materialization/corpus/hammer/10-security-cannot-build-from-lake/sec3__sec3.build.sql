-- hammer/10-security-cannot-build-from-lake/sec3/sec3.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source sneaky
-- sourceID: sneaky@<hammer>/sec3/sec3.malloy
-- connection: lake
-- sourceEntityId: b6198f2201a5f4d39b08283dcbc4e05ed5e46ff54570f38600f5609e4b1af71a
-- compiler key: b6198f2201a5f4d39b08283dcbc4e05ed5e46ff54570f38600f5609e4b1af71a (the sourceEntityId)
-- build SQL:
SELECT 
   base."x" as "x"
FROM (SELECT 1 AS x) as base
GROUP BY 1
ORDER BY 1 asc NULLS LAST
