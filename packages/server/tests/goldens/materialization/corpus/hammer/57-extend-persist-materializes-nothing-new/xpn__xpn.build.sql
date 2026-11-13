-- hammer/57-extend-persist-materializes-nothing-new/xpn/xpn.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/xpn/xpn.malloy
-- connection: orders_pg
-- sourceEntityId: bd29f84b198b7058b58a1cbef4774d845588da858e70d57921ee712eb72c23a4
-- compiler key: bd29f84b198b7058b58a1cbef4774d845588da858e70d57921ee712eb72c23a4 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "num_orders"
FROM public.xpn_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source daily_with_avg
-- sourceID: daily_with_avg@<hammer>/xpn/xpn.malloy
-- connection: orders_pg
-- sourceEntityId: bd29f84b198b7058b58a1cbef4774d845588da858e70d57921ee712eb72c23a4
-- compiler key: bd29f84b198b7058b58a1cbef4774d845588da858e70d57921ee712eb72c23a4 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "num_orders"
FROM public.xpn_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
