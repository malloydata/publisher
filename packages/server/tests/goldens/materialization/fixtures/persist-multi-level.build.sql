-- fixture: persist-multi-level: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders_base
-- sourceID: orders_base@<pkg>/multi_level.malloy
-- connection: duckdb
-- sourceEntityId: 1bb28858164cefd3a2649fb9cb621d7d9f68d5389410c61e7701a3b891f6d911
-- compiler key: 54d09a855ffdde69ba0ad3118d97631e8b6a42e40ec038c4b4e52846cc821315
-- build SQL:
SELECT 
   base."category" as "category",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM 'data/orders.csv' as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source orders_rollup
-- sourceID: orders_rollup@<pkg>/multi_level.malloy
-- connection: duckdb
-- reads: orders_base@<pkg>/multi_level.malloy
-- sourceEntityId: ae2e931a995f7c0422af9e9930b2a6012974e4ba5ae5ce6bb59261c6c6f0cdd2
-- compiler key: f16ff28010004bd1d26b6c3a5216d2417b23a6f8d3b723dc377db5dc6a89a7f9
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."category" as "category",
     COALESCE(SUM(base."amount"),0) as "total_amount"
  FROM 'data/orders.csv' as base
  GROUP BY 1
  ORDER BY 2 desc NULLS LAST
)
SELECT 
   COUNT(1) as "category_count"
FROM __stage0 as base
-- build SQL with every other planned source built:
SELECT 
   COUNT(1) as "category_count"
FROM built.orders_base as base
