-- conformance: joins under persist [storage]: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source line_totals
-- sourceID: line_totals@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: d80436e5dfcec1ac42a1775608b5cd4afb50af3bcbb9c7c6b66105b86943a782
-- compiler key: d80436e5dfcec1ac42a1775608b5cd4afb50af3bcbb9c7c6b66105b86943a782 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COUNT(l_0."line_id") as "lines",
   AVG(CASE WHEN l_0."line_id" IS NOT NULL THEN l_0."qty" END) as "avg_qty",
   COUNT(DISTINCT base."id") as "n"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
 LEFT JOIN (
  SELECT CAST(line_id AS BIGINT) AS line_id, CAST(order_id AS BIGINT) AS order_id,
         sku, CAST(qty AS BIGINT) AS qty
  FROM read_csv('data/lines.csv', header=true, all_varchar=true)
) AS l_0
  ON l_0."order_id"=base."id"
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source segment_totals
-- sourceID: segment_totals@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: a1e3ce7de9baaf4db09512ed00697f879397c2e2aa1922d40466c642862cb2bd
-- compiler key: a1e3ce7de9baaf4db09512ed00697f879397c2e2aa1922d40466c642862cb2bd (the sourceEntityId)
-- build SQL:
SELECT 
   c_0."segment" as "segment",
   COALESCE((
        SELECT SUM(a.val) as value
        FROM (
          SELECT UNNEST(list(distinct {key:base."id", val: base."amount"})) a
        )
      ),0) as "t"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
 LEFT JOIN (
  SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
         CAST(signup_date AS DATE) AS signup_date
  FROM read_csv('data/customers.csv', header=true, all_varchar=true)
) AS c_0
  ON c_0."id"=base."customer_id"
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source with_lines
-- sourceID: with_lines@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 056030f5bb7dcb6787ded4b651210a5f820a27afad3664d22caa8daab7b02aa3
-- compiler key: 056030f5bb7dcb6787ded4b651210a5f820a27afad3664d22caa8daab7b02aa3 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."region" as "region",
   base."amount" as "amount"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
