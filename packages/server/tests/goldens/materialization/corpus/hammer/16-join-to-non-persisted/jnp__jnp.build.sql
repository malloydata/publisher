-- hammer/16-join-to-non-persisted/jnp/jnp.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders
-- sourceID: orders@<hammer>/jnp/jnp.malloy
-- connection: orders_pg
-- sourceEntityId: 5fdef7ca019202125b9bf9738c51d8bad2b92220dc189123dae67de751141657
-- compiler key: 5fdef7ca019202125b9bf9738c51d8bad2b92220dc189123dae67de751141657 (the sourceEntityId)
-- build SQL:
SELECT 
   base."order_id" as "order_id",
   base."amount" as "amount",
   base."region_id" as "region_id"
FROM public.jnp_orders as base
GROUP BY 1,2,3
ORDER BY 1 asc NULLS LAST
