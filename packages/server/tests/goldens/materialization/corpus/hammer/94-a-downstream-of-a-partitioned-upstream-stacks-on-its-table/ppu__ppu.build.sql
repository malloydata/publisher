-- hammer/94-a-downstream-of-a-partitioned-upstream-stacks-on-its-table/ppu/ppu.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source orders_all
-- sourceID: orders_all@<hammer>/ppu/ppu.malloy
-- connection: orders_pg
-- sourceEntityId: 4ec213661900b95f0c62b5af954319decea2cf369c2cf55ca3f87c02866990c5
-- compiler key: 8a63d1881bfbddd03bd50f539d20f21c76919924c321214cc32e2ac0430da70b
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."order_id" as "order_id",
   base."org_id" as "org_id"
FROM (SELECT order_id, org_id, amount FROM public.ppu_orders) as base

-- ==== source rollup
-- sourceID: rollup@<hammer>/ppu/ppu.malloy
-- connection: orders_pg
-- reads: orders_all@<hammer>/ppu/ppu.malloy
-- sourceEntityId: ff3ca9a7ab0fd8044b9e6153c920629b13a945d2193f863242028fa682fb8b89
-- compiler key: ff3ca9a7ab0fd8044b9e6153c920629b13a945d2193f863242028fa682fb8b89 (the sourceEntityId)
-- build SQL:
WITH __stage0 AS (
  SELECT 
     base."amount" as "amount",
     base."order_id" as "order_id",
     base."org_id" as "org_id"
  FROM (SELECT order_id, org_id, amount FROM public.ppu_orders) as base
)
SELECT 
   base."org_id" as "org_id",
   COALESCE(SUM(base."amount"),0) as "total"
FROM __stage0 as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
-- build SQL with every other planned source built:
SELECT 
   base."org_id" as "org_id",
   COALESCE(SUM(base."amount"),0) as "total"
FROM built.orders_all as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST
