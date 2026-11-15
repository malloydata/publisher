-- hammer/114-a-chained-build-reads-its-own-intermediate-not-a-same-named-stored-source/chsh/other.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source mid
-- sourceID: mid@<hammer>/chsh/other.malloy
-- connection: orders_pg
-- sourceEntityId: c60a1c296206c555bb268cc320acddc1e51ae519676e66ffc55a76af673caa7a
-- compiler key: c60a1c296206c555bb268cc320acddc1e51ae519676e66ffc55a76af673caa7a (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_date" as "order_date",
   max(base."amount") as "v"
FROM public.chsh_orders as base
GROUP BY 1
ORDER BY 1 desc NULLS LAST
