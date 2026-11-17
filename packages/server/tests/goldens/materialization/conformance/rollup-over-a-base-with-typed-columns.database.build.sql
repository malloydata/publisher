-- conformance: rollup over a base with typed columns [database]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source typed__rollup__dec__dadb73aa
-- sourceID: typed__rollup__dec__dadb73aa@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: 2f2aed1542b2e149ed951f3e604311ce8e823dc4c0278a2e77709ed9b731b12a
-- compiler key: 2f2aed1542b2e149ed951f3e604311ce8e823dc4c0278a2e77709ed9b731b12a (the sourceEntityId)
-- build SQL:
SELECT 
   base."dec" as "dec",
   COUNT(1) as "n_by_dec__partial"
FROM (
  SELECT * FROM (VALUES
    (CAST('9007199254740993' AS BIGINT), CAST(1.25 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a', {'k': 1}, MAP {'x': 1}),
    (CAST('9007199254740991' AS BIGINT), CAST(1.25 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a', {'k': 2}, MAP {'y': 2}),
    (CAST('-9007199254740993' AS BIGINT), CAST(2.50 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-06-30 23:30:00+00', 'b', {'k': 3}, MAP {'z': 3})
  ) AS t(big, dec, tstz, g, st, mp)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source typed__rollup__g__b3e24831
-- sourceID: typed__rollup__g__b3e24831@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: 5c1c053fa54529388c833f7ecd094d930dbe6d35bbd41903bd6e2ea7b1ae870e
-- compiler key: 5c1c053fa54529388c833f7ecd094d930dbe6d35bbd41903bd6e2ea7b1ae870e (the sourceEntityId)
-- build SQL:
SELECT 
   base."g" as "g",
   COALESCE(SUM(base."big"),0) as "big_sum__partial",
   COALESCE(SUM(base."dec"),0) as "dec_sum__partial"
FROM (
  SELECT * FROM (VALUES
    (CAST('9007199254740993' AS BIGINT), CAST(1.25 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a', {'k': 1}, MAP {'x': 1}),
    (CAST('9007199254740991' AS BIGINT), CAST(1.25 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a', {'k': 2}, MAP {'y': 2}),
    (CAST('-9007199254740993' AS BIGINT), CAST(2.50 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-06-30 23:30:00+00', 'b', {'k': 3}, MAP {'z': 3})
  ) AS t(big, dec, tstz, g, st, mp)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source typed__rollup__tstz__7a053bfd
-- sourceID: typed__rollup__tstz__7a053bfd@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: c444edfd5a5741c760d9495684adbea67c59fc03012797c891f6ac0ae9f3eaa1
-- compiler key: c444edfd5a5741c760d9495684adbea67c59fc03012797c891f6ac0ae9f3eaa1 (the sourceEntityId)
-- build SQL:
SELECT 
   base."tstz" as "tstz",
   COUNT(1) as "n_by_tstz__partial"
FROM (
  SELECT * FROM (VALUES
    (CAST('9007199254740993' AS BIGINT), CAST(1.25 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a', {'k': 1}, MAP {'x': 1}),
    (CAST('9007199254740991' AS BIGINT), CAST(1.25 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a', {'k': 2}, MAP {'y': 2}),
    (CAST('-9007199254740993' AS BIGINT), CAST(2.50 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-06-30 23:30:00+00', 'b', {'k': 3}, MAP {'z': 3})
  ) AS t(big, dec, tstz, g, st, mp)
) as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy.preaggregate.malloy
##! experimental.persistence
import { typed__rollup_base is typed } from "m.malloy"

#@ persist
source: typed__rollup__dec__dadb73aa is typed__rollup_base -> {
  group_by:
    dec
  aggregate:
    n_by_dec__partial is count()
}

#@ persist
source: typed__rollup__g__b3e24831 is typed__rollup_base -> {
  group_by:
    g
  aggregate:
    big_sum__partial is big.sum()
    dec_sum__partial is dec.sum()
}

#@ persist
source: typed__rollup__tstz__7a053bfd is typed__rollup_base -> {
  group_by:
    tstz
  aggregate:
    n_by_tstz__partial is count()
}
