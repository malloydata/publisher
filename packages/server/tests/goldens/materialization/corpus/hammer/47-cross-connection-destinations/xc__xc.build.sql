-- hammer/47-cross-connection-destinations/xc/xc.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source a
-- sourceID: a@<hammer>/xc/xc.malloy
-- connection: orders_pg
-- sourceEntityId: d03f3258bd862108f5b80f2cf9a8df6168cdbd16acef45948725101095676270
-- compiler key: d03f3258bd862108f5b80f2cf9a8df6168cdbd16acef45948725101095676270 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.xc_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source b
-- sourceID: b@<hammer>/xc/xc.malloy
-- connection: orders_pg
-- sourceEntityId: 8e4c34b6a1e9baa72f662ee6da149e30bc434dceb97e9705c31174b7eaae414d
-- compiler key: 8e4c34b6a1e9baa72f662ee6da149e30bc434dceb97e9705c31174b7eaae414d (the sourceEntityId)
-- build SQL:
SELECT 
   COALESCE(SUM(base."amount"),0) as "grand_total"
FROM public.xc_orders as base
