-- hammer/125-a-list-column-is-stored-and-served/mzl/mzl.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source s_arr
-- sourceID: s_arr@<hammer>/mzl/mzl.malloy
-- connection: orders_pg
-- sourceEntityId: bee3a89c3305eac4960a694fd6fc52a9219664cf3ec172949ff6848339e3af4d
-- compiler key: bee3a89c3305eac4960a694fd6fc52a9219664cf3ec172949ff6848339e3af4d (the sourceEntityId)
-- build SQL:
SELECT 
   base."station_id" as "station_id",
   base."v" as "v"
FROM (
  SELECT station_id,
         array_agg(DISTINCT subscriber_type ORDER BY subscriber_type) AS v
  FROM public.mzl_trips
  GROUP BY 1
) as base

-- generated files

-- ==== <hammer>/mzl/mzl.malloy.materialize.malloy
##! experimental.persistence
import { s_arr__mat__base is s_arr } from 'mzl.malloy'

#@ persist name="s_arr" storage="lake"
source: s_arr__mat is s_arr__mat__base -> {
  select:
    station_id,
    v
}
