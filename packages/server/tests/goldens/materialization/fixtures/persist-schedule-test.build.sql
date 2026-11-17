-- fixture: persist-schedule-test: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source order_summary
-- sourceID: order_summary@<pkg>/persist_schedule_test.malloy
-- connection: duckdb
-- sourceEntityId: 218fa4ffa546f2a111269e08a2004f00e5f76922c05e984890204e4a53107cb9
-- compiler key: 218fa4ffa546f2a111269e08a2004f00e5f76922c05e984890204e4a53107cb9 (the sourceEntityId)
-- build SQL:
SELECT 
   base."category" as "category",
   COUNT(1) as "total_orders",
   COALESCE(SUM(base."amount"),0) as "total_revenue"
FROM 'data/orders.csv' as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
