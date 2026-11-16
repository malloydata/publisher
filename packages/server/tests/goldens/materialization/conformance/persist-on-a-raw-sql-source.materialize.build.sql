-- conformance: persist on a raw SQL source [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source raw_direct
-- sourceID: raw_direct@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: d315aa74c60e26bdae2de1c53ed1a5dbd279d607ad66f56d81a4684721304742
-- compiler key: d315aa74c60e26bdae2de1c53ed1a5dbd279d607ad66f56d81a4684721304742 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   base."amount" as "amount"
FROM (
  SELECT region, CAST(amount AS DOUBLE) AS amount
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { raw_direct__mat__base is raw_direct } from 'm.malloy'

#@ persist name="raw_direct" storage="lake"
source: raw_direct__mat is raw_direct__mat__base -> {
  select:
    region,
    amount
}
