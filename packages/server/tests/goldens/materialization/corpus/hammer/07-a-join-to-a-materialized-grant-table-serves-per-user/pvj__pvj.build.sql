-- hammer/07-a-join-to-a-materialized-grant-table-serves-per-user/pvj/pvj.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source accounts
-- sourceID: accounts@<hammer>/pvj/pvj.malloy
-- connection: orders_pg
-- sourceEntityId: 89ec4999e1b9f6bef8953b82cd7ce82839af2d05c76fa9bc812241d7c5bee240
-- compiler key: fd23eb961ebce416e2efbcfa822d44848635eb2003d7711ddda1abd7d4126195
-- build SQL:
SELECT 
   base."account_id" as "account_id",
   base."org_id" as "org_id"
FROM (SELECT account_id, org_id FROM public.pvj_accounts) as base

-- ==== source grants
-- sourceID: grants@<hammer>/pvj/pvj.malloy
-- connection: orders_pg
-- sourceEntityId: aebecd89bf95d910909071b0ff7d0040391621b03eb2c155f1d90b07f74bf391
-- compiler key: ddb6090be76dc7348a5b30e0548f6ef2e98836a942916176c986d167b8a36055
-- build SQL:
SELECT 
   base."opp_id" as "opp_id",
   base."org_id" as "org_id",
   base."user_id" as "user_id"
FROM (SELECT org_id, user_id, opp_id FROM public.pvj_grants) as base

-- ==== source opps
-- sourceID: opps@<hammer>/pvj/pvj.malloy
-- connection: orders_pg
-- reads: grants@<hammer>/pvj/pvj.malloy
-- sourceEntityId: fafe57aed7ddd235121cdf2ddc5e108819ac41011415b6cbbd417a75a318825d
-- compiler key: 74b09e2476274c68e976da42e43118968918ab2bb3316d322c27e043910d4b12
-- build SQL:
SELECT 
   base."amount" as "amount",
   base."opp_id" as "opp_id",
   base."org_id" as "org_id",
   base."restricted" as "restricted"
FROM (SELECT opp_id, org_id, restricted, amount FROM public.pvj_opps) as base
