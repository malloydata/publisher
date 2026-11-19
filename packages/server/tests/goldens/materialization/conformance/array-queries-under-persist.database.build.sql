-- conformance: array queries under persist [database]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source cart_totals
-- sourceID: cart_totals@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 131306c8b9be0ddadfbb34a5dbe0b7554c1d3ebf27c2c572eac40b1caccd1031
-- compiler key: 131306c8b9be0ddadfbb34a5dbe0b7554c1d3ebf27c2c572eac40b1caccd1031 (the sourceEntityId)
-- build SQL:
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

-- ==== source cart_rows
-- sourceID: cart_rows@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: e12bf1e1b807417625a7648c73111597f4d49379378e6807b8a8bb105f5ae6a4
-- compiler key: e12bf1e1b807417625a7648c73111597f4d49379378e6807b8a8bb105f5ae6a4 (the sourceEntityId)
-- build SQL:
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
