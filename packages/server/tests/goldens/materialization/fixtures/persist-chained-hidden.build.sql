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

-- ==== source west_spend
-- sourceID: west_spend@<pkg>/chained_hidden.malloy
-- connection: duckdb
-- reads: customers_hidden@<pkg>/chained_hidden.malloy
-- sourceEntityId: f00b9c12fbf1334a5f57a674598c3026223c7009fb28793e4a1b12e121aa4572
-- compiler key: f00b9c12fbf1334a5f57a674598c3026223c7009fb28793e4a1b12e121aa4572 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
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
)
SELECT 
   base."segment" as "segment",
   COALESCE(SUM(CASE WHEN base."home_region"='west' THEN base."amount" END),0) as "west_total"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   base."segment" as "segment",
   COALESCE(SUM(CASE WHEN base."home_region"='west' THEN base."amount" END),0) as "west_total"
FROM built.customers_hidden as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
