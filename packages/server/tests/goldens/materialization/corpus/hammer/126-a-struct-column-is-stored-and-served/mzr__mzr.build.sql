-- hammer/126-a-struct-column-is-stored-and-served/mzr/mzr.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source events
-- sourceID: events@<hammer>/mzr/mzr.malloy
-- connection: lake_probe
-- sourceEntityId: d2d17e0403609ed0a9f6e9a224e7e124c13f72cce10e825ee0f90d56bce3a52c
-- compiler key: d2d17e0403609ed0a9f6e9a224e7e124c13f72cce10e825ee0f90d56bce3a52c (the sourceEntityId)
-- build SQL:
SELECT 
   base."event_id" as "event_id",
   base."device" as "device",
   base."geo" as "geo",
   base."params" as "params"
FROM lake_probe.main.mzr_events as base

-- generated files

-- ==== <hammer>/mzr/mzr.malloy.materialize.malloy
##! experimental.persistence
import { events__mat__base is events } from 'mzr.malloy'

#@ persist name="events" storage="lake"
source: events__mat is events__mat__base -> {
  select:
    event_id,
    device,
    geo,
    params
}
