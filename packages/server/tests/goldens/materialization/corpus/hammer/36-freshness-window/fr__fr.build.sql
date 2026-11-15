-- hammer/36-freshness-window/fr/fr.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source daily
-- sourceID: daily@<hammer>/fr/fr.malloy
-- connection: orders_pg
-- sourceEntityId: 6e0877f0cab0fb85aafffc8b270645fc000086295e298f466373ab7f1ec20b05
-- compiler key: 6e0877f0cab0fb85aafffc8b270645fc000086295e298f466373ab7f1ec20b05 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COALESCE(SUM(base."amount"),0) as "total_amount"
FROM public.fr_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
