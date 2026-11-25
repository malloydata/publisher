-- hammer/03-the-derived-names-configure-a-publisher/sdn/sdn.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source alpha
-- sourceID: alpha@<hammer>/sdn/sdn.malloy
-- connection: orders_pg
-- sourceEntityId: a8702976689f31731c0c2904586ca9bb0390bd723fd0813d853bbb3bfd497663
-- compiler key: a8702976689f31731c0c2904586ca9bb0390bd723fd0813d853bbb3bfd497663 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   COUNT(1) as "n"
FROM public.sdn_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST

-- ==== source beta
-- sourceID: beta@<hammer>/sdn/sdn.malloy
-- connection: orders_pg
-- sourceEntityId: 0dcc17e646267ab8d386e6c77720e6540a83bc187e56d0e37f0e12754b6dd654
-- compiler key: 0dcc17e646267ab8d386e6c77720e6540a83bc187e56d0e37f0e12754b6dd654 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "total"
FROM public.sdn_orders as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
