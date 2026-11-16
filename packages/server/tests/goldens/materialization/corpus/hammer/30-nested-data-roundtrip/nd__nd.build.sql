-- hammer/30-nested-data-roundtrip/nd/nd.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/nd/nd.malloy
-- connection: orders_pg
-- sourceEntityId: 2b4a5fd3342d0b1021f845a61be2f630e13af7d2f8e203eaa1bad0ce7a2667c3
-- compiler key: 2b4a5fd3342d0b1021f845a61be2f630e13af7d2f8e203eaa1bad0ce7a2667c3 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT
    group_set,
    base."order_date" as "order_date__0",
    CASE WHEN group_set=0 THEN
      COALESCE(SUM(base."amount"),0)
      END as "total_amount__0",
    CASE WHEN group_set=1 THEN
      base."region"
      END as "region__1",
    CASE WHEN group_set=1 THEN
      COALESCE(SUM(base."amount"),0)
      END as "region_total__1"
  FROM public.nd_orders as base
  CROSS JOIN (SELECT UNNEST(GENERATE_SERIES(0,1,1)) as group_set  ) as group_set
  GROUP BY 1,2,4
)
SELECT
  "order_date__0" as "order_date",
  MAX(CASE WHEN group_set=0 THEN "total_amount__0" END) as "total_amount",
  COALESCE(LIST({
    "region": "region__1", 
    "region_total": "region_total__1"}  ORDER BY  "region__1" ASC NULLS LAST) FILTER (WHERE group_set=1),[]) as "by_region"
FROM __stage0
GROUP BY 1
ORDER BY 1 desc NULLS LAST
