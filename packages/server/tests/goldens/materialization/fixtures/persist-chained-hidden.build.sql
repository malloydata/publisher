-- fixture: persist-chained-hidden: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source customers_hidden
-- sourceID: customers_hidden@<pkg>/chained_hidden.malloy
-- connection: duckdb
-- sourceEntityId: e201ceaf6812d2eb1a1fd36a43cee466300f3540098013c0d0eb24d60a75f064
-- compiler key: e201ceaf6812d2eb1a1fd36a43cee466300f3540098013c0d0eb24d60a75f064 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."segment" as "segment",
   base."home_region" as "home_region",
   base."amount" as "amount"
FROM (
  SELECT * FROM (VALUES
    (1, 'a', 'west', 10.0),
    (2, 'b', 'east', 5.0),
    (3, 'a', 'west', 7.0)
  ) AS t(id, segment, home_region, amount)
) as base
