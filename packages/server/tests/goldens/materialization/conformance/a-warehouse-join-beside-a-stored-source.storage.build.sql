-- conformance: a warehouse join beside a stored source [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source cust_b
-- sourceID: cust_b@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 820d6af004bafcff7762f225f8837cdf222b2d7dfa966299a5f66b074f48d280
-- compiler key: 820d6af004bafcff7762f225f8837cdf222b2d7dfa966299a5f66b074f48d280 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."segment" as "segment"
FROM (
  SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
         CAST(signup_date AS DATE) AS signup_date
  FROM read_csv('data/customers.csv', header=true, all_varchar=true)
) as base
