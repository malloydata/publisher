-- feature: storage placement, an empty destination on a v1 persist: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source p_empty
-- sourceID: p_empty@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: abb899ecd520649537a3b9696e66dd32a8c726e4c1b961ca0cc7f7d797a578c6
-- compiler key: abb899ecd520649537a3b9696e66dd32a8c726e4c1b961ca0cc7f7d797a578c6 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COUNT(1) as "n"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
