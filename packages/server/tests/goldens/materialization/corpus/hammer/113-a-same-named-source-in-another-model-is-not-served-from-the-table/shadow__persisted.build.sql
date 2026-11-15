-- hammer/113-a-same-named-source-in-another-model-is-not-served-from-the-table/shadow/persisted.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/shadow/persisted.malloy
-- connection: orders_pg
-- sourceEntityId: 6b3756a5d555cfcbfe71a470eef341f9605dfbcaf40457429a985988860d8717
-- compiler key: 6b3756a5d555cfcbfe71a470eef341f9605dfbcaf40457429a985988860d8717 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.shadow_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
