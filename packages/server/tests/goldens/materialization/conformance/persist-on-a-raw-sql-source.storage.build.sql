-- conformance: persist on a raw SQL source [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source raw_direct
-- sourceID: raw_direct@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 1e1f98e3727ab4f0ce7de85d34d4f237f7a11123dd46e7b8a969abec8bbe672e
-- compiler key: 1e1f98e3727ab4f0ce7de85d34d4f237f7a11123dd46e7b8a969abec8bbe672e (the sourceEntityId)
-- build SQL:
SELECT region, CAST(amount AS DOUBLE) AS amount
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
