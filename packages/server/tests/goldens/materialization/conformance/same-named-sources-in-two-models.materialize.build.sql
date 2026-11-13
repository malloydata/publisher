-- conformance: same-named sources in two models [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- (no planned source)

-- generated files

-- ==== <pkg>/a.malloy.materialize.malloy
##! experimental.persistence
import { summary__mat__base is summary } from 'a.malloy'

#@ persist name="summary" storage="lake"
source: summary__mat is summary__mat__base -> {
  select:
    region,
    total
}

-- ==== <pkg>/b.malloy.materialize.malloy
##! experimental.persistence
import { summary__mat__base is summary } from 'b.malloy'

#@ persist name="summary" storage="lake"
source: summary__mat is summary__mat__base -> {
  select:
    region,
    total
}
