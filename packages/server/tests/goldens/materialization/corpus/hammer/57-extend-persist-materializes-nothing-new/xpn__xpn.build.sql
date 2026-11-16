-- hammer/57-extend-persist-materializes-nothing-new/xpn/xpn.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/xpn/xpn.malloy
-- connection: orders_pg
-- sourceEntityId: bbd710d6e522f2ecb13f6cc756060a32752660ce8839ed176ebbd7b0729c51c7
-- compiler key: bd29f84b198b7058b58a1cbef4774d845588da858e70d57921ee712eb72c23a4
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
-- sourceEntityId: a488ac5a2b6eb1a61248c5c7a8343146ebecc0ddf02bd092ae29fd8203ba5e21
-- compiler key: bd29f84b198b7058b58a1cbef4774d845588da858e70d57921ee712eb72c23a4
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount",
   COUNT(1) as "num_orders"
FROM public.xpn_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
