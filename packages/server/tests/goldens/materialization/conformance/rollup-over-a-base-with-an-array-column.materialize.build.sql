-- conformance: rollup over a base with an array column [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source listed__rollup__g__ab53ae01
-- sourceID: listed__rollup__g__ab53ae01@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: b906e0e0262575336fd7818bd022af2ec363e5c343e122a47563845a657351bd
-- compiler key: b906e0e0262575336fd7818bd022af2ec363e5c343e122a47563845a657351bd (the sourceEntityId)
-- build SQL:
SELECT 
   base."g" as "g",
   COALESCE(SUM(base."amount"),0) as "total__partial"
FROM (
  SELECT * FROM (VALUES ('a', 1.5, [1, 2]), ('a', 2.0, [3]), ('b', 4.0, []))
    AS t(g, amount, lst)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy.preaggregate.malloy
##! experimental.persistence
import { listed__rollup_base is listed } from "m.malloy"

#@ persist storage="lake"
source: listed__rollup__g__ab53ae01 is listed__rollup_base -> {
  group_by:
    g
  aggregate:
    total__partial is amount.sum()
}
