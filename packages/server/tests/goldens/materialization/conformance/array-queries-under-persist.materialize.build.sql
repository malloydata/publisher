-- conformance: array queries under persist [materialize]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source cart_rows
-- sourceID: cart_rows@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 94337d64da6f769f74675d99ec9a24e277d11f5b4659313035fad98e3123a032
-- compiler key: 94337d64da6f769f74675d99ec9a24e277d11f5b4659313035fad98e3123a032 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."id" as "id",
     base."g" as "g",
     base."items" as "items"
  FROM (
    SELECT * FROM (VALUES
      (1, 'a', [{'sku': 'x', 'price': 1.5}, {'sku': 'y', 'price': 2.0}]),
      (2, 'b', [{'sku': 'x', 'price': 4.0}]),
      (3, 'a', [])
    ) AS t(id, g, items)
  ) as base
)
SELECT 
   base."id" as "id",
   base."g" as "g",
   base."items" as "items"
FROM __stage0 as base

-- ==== source cart_totals
-- sourceID: cart_totals@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: b8c77013015fb5c71865cb71ce01dce1056f7ae465d383a91776c24f2ca6e286
-- compiler key: b8c77013015fb5c71865cb71ce01dce1056f7ae465d383a91776c24f2ca6e286 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."id" as "id",
     base."g" as "g",
     COALESCE(LIST_SUM(LIST_TRANSFORM(base."items", __items_element_0 -> __items_element_0."price")), 0) as "t"
  FROM (
    SELECT * FROM (VALUES
      (1, 'a', [{'sku': 'x', 'price': 1.5}, {'sku': 'y', 'price': 2.0}]),
      (2, 'b', [{'sku': 'x', 'price': 4.0}]),
      (3, 'a', [])
    ) AS t(id, g, items)
  ) as base
)
SELECT 
   base."id" as "id",
   base."g" as "g",
   base."t" as "t"
FROM __stage0 as base

-- generated files

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { cart_rows__mat__base is cart_rows } from 'm.malloy'
import { cart_totals__mat__base is cart_totals } from 'm.malloy'

#@ persist name="cart_rows" storage="lake"
source: cart_rows__mat is cart_rows__mat__base -> {
  select:
    id,
    g,
    items
}

#@ persist name="cart_totals" storage="lake"
source: cart_totals__mat is cart_totals__mat__base -> {
  select:
    id,
    g,
    t
}
