-- hammer/48-off-serves-live/obp/obp.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/obp/obp.malloy
-- connection: orders_pg
-- sourceEntityId: b31f2f709776b803834dd6982a95182b59a368d0ba250f4cae49447f50b8e918
-- compiler key: b31f2f709776b803834dd6982a95182b59a368d0ba250f4cae49447f50b8e918 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.obp_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
