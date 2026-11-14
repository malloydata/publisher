-- hammer/105-every-reading-of-a-filtered-parent-applies-its-filter/frp/frp.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source deals
-- sourceID: deals@<hammer>/frp/frp.malloy
-- connection: orders_pg
-- sourceEntityId: 9dce68668f4c9afdd11326365c251098976f35118dcac86137845d120b1606eb
-- compiler key: 9dce68668f4c9afdd11326365c251098976f35118dcac86137845d120b1606eb (the sourceEntityId)
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."deal_id" as "deal_id",
   base."is_deleted" as "is_deleted",
   base."is_open" as "is_open"
FROM public.frp_deals as base

-- ==== source pipeline_now
-- sourceID: pipeline_now@<hammer>/frp/frp.malloy
-- connection: orders_pg
-- sourceEntityId: 9dce68668f4c9afdd11326365c251098976f35118dcac86137845d120b1606eb
-- compiler key: 9dce68668f4c9afdd11326365c251098976f35118dcac86137845d120b1606eb (the sourceEntityId)
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."deal_id" as "deal_id",
   base."is_deleted" as "is_deleted",
   base."is_open" as "is_open"
FROM public.frp_deals as base

-- ==== source deals_alias
-- sourceID: deals_alias@<hammer>/frp/frp.malloy
-- connection: orders_pg
-- sourceEntityId: 9dce68668f4c9afdd11326365c251098976f35118dcac86137845d120b1606eb
-- compiler key: 9dce68668f4c9afdd11326365c251098976f35118dcac86137845d120b1606eb (the sourceEntityId)
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."deal_id" as "deal_id",
   base."is_deleted" as "is_deleted",
   base."is_open" as "is_open"
FROM public.frp_deals as base

-- ==== source x
-- sourceID: x@<hammer>/frp/frp.malloy
-- connection: orders_pg
-- reads: deals@<hammer>/frp/frp.malloy, pipeline_now@<hammer>/frp/frp.malloy, deals_alias@<hammer>/frp/frp.malloy
-- sourceEntityId: 83831f3a9aa8a6acd61b001f72557a76b91abf07a388b09cfb13f139d4188f7d
-- compiler key: 83831f3a9aa8a6acd61b001f72557a76b91abf07a388b09cfb13f139d4188f7d (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."amount" as "amount",
     base."deal_id" as "deal_id",
     base."is_deleted" as "is_deleted",
     base."is_open" as "is_open"
  FROM public.frp_deals as base
)
SELECT 
   COALESCE(SUM(base."amount"),0) as "total"
FROM __stage0 as base
WHERE (COALESCE(NOT base."is_deleted",TRUE))
AND (base."is_open")
-- build SQL with every other planned source built:
SELECT 
   COALESCE(SUM(base."amount"),0) as "total"
FROM built.deals_alias as base
WHERE (COALESCE(NOT base."is_deleted",TRUE))
AND (base."is_open")

-- ==== source y
-- sourceID: y@<hammer>/frp/frp.malloy
-- connection: orders_pg
-- reads: deals@<hammer>/frp/frp.malloy, pipeline_now@<hammer>/frp/frp.malloy, deals_alias@<hammer>/frp/frp.malloy
-- sourceEntityId: d256b2d677d1669f14fabbc68e182911b847491cc66693b57dc1ea147546bd71
-- compiler key: d256b2d677d1669f14fabbc68e182911b847491cc66693b57dc1ea147546bd71 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."amount" as "amount",
     base."deal_id" as "deal_id",
     base."is_deleted" as "is_deleted",
     base."is_open" as "is_open"
  FROM public.frp_deals as base
)
SELECT 
   COALESCE(SUM(base."amount"),0) as "total"
FROM __stage0 as base
WHERE COALESCE(NOT base."is_deleted",TRUE)
-- build SQL with every other planned source built:
SELECT 
   COALESCE(SUM(base."amount"),0) as "total"
FROM built.deals_alias as base
WHERE COALESCE(NOT base."is_deleted",TRUE)

-- ==== source z
-- sourceID: z@<hammer>/frp/frp.malloy
-- connection: orders_pg
-- reads: deals@<hammer>/frp/frp.malloy, pipeline_now@<hammer>/frp/frp.malloy, deals_alias@<hammer>/frp/frp.malloy
-- sourceEntityId: 7fb062ba34fdf1e0f477bf692648688cde2a49d50e1465ea3cb238400364d387
-- compiler key: 7fb062ba34fdf1e0f477bf692648688cde2a49d50e1465ea3cb238400364d387 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."amount" as "amount",
     base."deal_id" as "deal_id",
     base."is_deleted" as "is_deleted",
     base."is_open" as "is_open"
  FROM public.frp_deals as base
)
SELECT 
   COALESCE(SUM(base."amount"),0) as "total",
   COUNT(1) as "n"
FROM __stage0 as base
WHERE COALESCE(NOT base."is_deleted",TRUE)
-- build SQL with every other planned source built:
SELECT 
   COALESCE(SUM(base."amount"),0) as "total",
   COUNT(1) as "n"
FROM built.deals_alias as base
WHERE COALESCE(NOT base."is_deleted",TRUE)

-- ==== source w
-- sourceID: w@<hammer>/frp/frp.malloy
-- connection: orders_pg
-- reads: deals@<hammer>/frp/frp.malloy, pipeline_now@<hammer>/frp/frp.malloy, deals_alias@<hammer>/frp/frp.malloy
-- sourceEntityId: cf4461896a5a940f45f0c599155081dacaa0ffd03f1f54e78e9b2c8d7548daa2
-- compiler key: cf4461896a5a940f45f0c599155081dacaa0ffd03f1f54e78e9b2c8d7548daa2 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."amount" as "amount",
     base."deal_id" as "deal_id",
     base."is_deleted" as "is_deleted",
     base."is_open" as "is_open"
  FROM public.frp_deals as base
)
, __stage1 AS (
  SELECT 
     base."amount" as "amount",
     base."deal_id" as "deal_id",
     base."is_deleted" as "is_deleted",
     base."is_open" as "is_open"
  FROM __stage0 as base
  WHERE COALESCE(NOT base."is_deleted",TRUE)
)
SELECT 
   COALESCE(SUM(base."amount"),0) as "total"
FROM __stage1 as base
-- build SQL with every other planned source built:
WITH __stage0 AS (
  SELECT 
     base."amount" as "amount",
     base."deal_id" as "deal_id",
     base."is_deleted" as "is_deleted",
     base."is_open" as "is_open"
  FROM built.deals_alias as base
  WHERE COALESCE(NOT base."is_deleted",TRUE)
)
SELECT 
   COALESCE(SUM(base."amount"),0) as "total"
FROM __stage0 as base
