-- fixture: persist-schedule-warn-test: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source order_summary
-- sourceID: order_summary@<pkg>/persist_warn_test.malloy
-- connection: duckdb
-- sourceEntityId: 4ab942004fa34a997237fa2e6b732c6b53cf7454f079c4345dea24a95d653bc6
-- compiler key: 218fa4ffa546f2a111269e08a2004f00e5f76922c05e984890204e4a53107cb9
-- build SQL:
SELECT 
   base."category" as "category",
   COUNT(1) as "total_orders",
   COALESCE(SUM(base."amount"),0) as "total_revenue"
FROM 'data/orders.csv' as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
