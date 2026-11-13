-- conformance: a warehouse join beside a stored source [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source cust_b
-- sourceID: cust_b@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: f234b5b635601c6ab30e41520cbfb0ca5dea189f42b4cf1a6e29af2245250b6a
-- compiler key: f234b5b635601c6ab30e41520cbfb0ca5dea189f42b4cf1a6e29af2245250b6a (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."id" as "id",
     base."segment" as "segment"
  FROM (
    SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
           CAST(signup_date AS DATE) AS signup_date
    FROM read_csv('data/customers.csv', header=true, all_varchar=true)
  ) as base
)
SELECT 
   base."id" as "id",
   base."segment" as "segment"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { cust_b__mat__base is cust_b } from 'm.malloy'

#@ persist name="cust_b" storage="lake"
source: cust_b__mat is cust_b__mat__base -> {
  select:
    id,
    segment
}
