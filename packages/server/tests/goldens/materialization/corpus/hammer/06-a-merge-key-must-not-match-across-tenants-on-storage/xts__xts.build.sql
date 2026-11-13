-- hammer/06-a-merge-key-must-not-match-across-tenants-on-storage/xts/xts.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source order_rows
-- sourceID: order_rows@<hammer>/xts/xts.malloy
-- connection: orders_pg
-- sourceEntityId: b1a2b61da96c25a55ffbd095304b951ce78f7a5bddcd02cff0286545b0465ff4
-- compiler key: b1a2b61da96c25a55ffbd095304b951ce78f7a5bddcd02cff0286545b0465ff4 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."org_id" as "org_id",
   base."batch" as "batch",
   COALESCE(SUM(base."amount"),0) as "amount_total"
FROM public.xts_orders as base
GROUP BY 1,2,3
ORDER BY 4 desc NULLS LAST
