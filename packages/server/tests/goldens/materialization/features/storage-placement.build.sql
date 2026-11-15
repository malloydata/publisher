-- feature: storage placement: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source m_storage
-- sourceID: m_storage@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: eae11fcde4b5b91f7571923a182628d686e43b4d324795e8e63d896255f534eb
-- compiler key: eae11fcde4b5b91f7571923a182628d686e43b4d324795e8e63d896255f534eb (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."customer_id" as "customer_id",
   base."region" as "region",
   base."status" as "status",
   base."amount" as "amount",
   base."discount" as "discount",
   base."order_date" as "order_date",
   base."created_at" as "created_at",
   base."order" as "order"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base

-- ==== source views__v_sibling__view
-- sourceID: views__v_sibling__view@<pkg>/m.malloy.persist-view.malloy
-- connection: duckdb
-- sourceEntityId: abb899ecd520649537a3b9696e66dd32a8c726e4c1b961ca0cc7f7d797a578c6
-- compiler key: abb899ecd520649537a3b9696e66dd32a8c726e4c1b961ca0cc7f7d797a578c6 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COUNT(1) as "n"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source views__v_nested__view
-- sourceID: views__v_nested__view@<pkg>/m.malloy.persist-view.malloy
-- connection: duckdb
-- sourceEntityId: 5581630f251fc69bee0819ff474a90427bd2332f845afe9236e0fc1b1bcee5e4
-- compiler key: 5581630f251fc69bee0819ff474a90427bd2332f845afe9236e0fc1b1bcee5e4 (the sourceEntityId)
-- build SQL:
SELECT 
   base."status" as "status",
   COUNT(1) as "n"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source rollups__rollup__region__495e6697
-- sourceID: rollups__rollup__region__495e6697@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: e7124d2ae508fd5155a0b0924e330916ddb262bdfae41dc5209e342abd15a4ed
-- compiler key: e7124d2ae508fd5155a0b0924e330916ddb262bdfae41dc5209e342abd15a4ed (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "r_storage__partial"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source v1_rollups__rollup__region__cbe05f06
-- sourceID: v1_rollups__rollup__region__cbe05f06@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: 42131d0e5306574e7c19036d724302cd2fc2bffba934d06c111a35fe843cd722
-- compiler key: 42131d0e5306574e7c19036d724302cd2fc2bffba934d06c111a35fe843cd722 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COALESCE(SUM(base."amount"),0) as "v1_sibling__partial"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source v1_rollups__rollup__status__1425197f
-- sourceID: v1_rollups__rollup__status__1425197f@<pkg>/m.malloy.preaggregate.malloy
-- connection: duckdb
-- sourceEntityId: 72701c6a3e1f4dea81ae567074bc95701b01bf347a59e6f1213025eaa962fff1
-- compiler key: 72701c6a3e1f4dea81ae567074bc95701b01bf347a59e6f1213025eaa962fff1 (the sourceEntityId)
-- build SQL:
SELECT 
   base."status" as "status",
   max(base."amount") as "v1_nested__partial"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source p_sibling
-- sourceID: p_sibling@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: abb899ecd520649537a3b9696e66dd32a8c726e4c1b961ca0cc7f7d797a578c6
-- compiler key: abb899ecd520649537a3b9696e66dd32a8c726e4c1b961ca0cc7f7d797a578c6 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COUNT(1) as "n"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source p_nested
-- sourceID: p_nested@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: abb899ecd520649537a3b9696e66dd32a8c726e4c1b961ca0cc7f7d797a578c6
-- compiler key: abb899ecd520649537a3b9696e66dd32a8c726e4c1b961ca0cc7f7d797a578c6 (the sourceEntityId)
-- build SQL:
SELECT 
   base."region" as "region",
   COUNT(1) as "n"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source p_named_sibling
-- sourceID: p_named_sibling@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 5581630f251fc69bee0819ff474a90427bd2332f845afe9236e0fc1b1bcee5e4
-- compiler key: 5581630f251fc69bee0819ff474a90427bd2332f845afe9236e0fc1b1bcee5e4 (the sourceEntityId)
-- build SQL:
SELECT 
   base."status" as "status",
   COUNT(1) as "n"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- ==== source p_named_nested
-- sourceID: p_named_nested@<pkg>/m.malloy
-- connection: duckdb
-- sourceEntityId: 5581630f251fc69bee0819ff474a90427bd2332f845afe9236e0fc1b1bcee5e4
-- compiler key: 5581630f251fc69bee0819ff474a90427bd2332f845afe9236e0fc1b1bcee5e4 (the sourceEntityId)
-- build SQL:
SELECT 
   base."status" as "status",
   COUNT(1) as "n"
FROM (
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
) as base
GROUP BY 1
ORDER BY 2 desc NULLS LAST

-- generated files

-- ==== <pkg>/m.malloy
##! experimental { persistence composite_sources }
import "raw.malloy"

                         
source: p_sibling is orders_raw -> { group_by: region; aggregate: n is count() }

                             
source: p_nested is orders_raw -> { group_by: region; aggregate: n is count() }

                                        
source: p_named_sibling is orders_raw -> { group_by: status; aggregate: n is count() }

                                                   
source: p_named_nested is orders_raw -> { group_by: status; aggregate: n is count() }

#(materialize) storage="lake"
source: m_storage is orders_raw extend { measure: n is count() }

source: views is orders_raw extend {
  measure: n is count()
  #@ persist storage="lake"
  view: v_sibling is { group_by: region; aggregate: n }
  #@ persist { storage="lake" }
  view: v_nested is { group_by: status; aggregate: n }
}

source: rollups is orders_raw extend {
  #(preaggregate) grain=["region"] storage="lake"
  measure: r_storage is amount.sum()
}

source: v1_rollups is orders_raw extend {
  #@ preaggregate grain="region" storage="lake"
  measure: v1_sibling is amount.sum()
  #@ preaggregate { grain="status" storage="lake" }
  measure: v1_nested is amount.max()
}

-- ==== <pkg>/m.malloy.materialize.malloy
##! experimental.persistence
import { m_storage__mat__base is m_storage } from 'm.malloy'

#@ persist name="m_storage" storage="lake"
source: m_storage__mat is m_storage__mat__base -> {
  select:
    id,
    customer_id,
    region,
    status,
    amount,
    discount,
    order_date,
    created_at,
    order
}

-- ==== <pkg>/m.malloy.persist-view.malloy
##! experimental.persistence
import { views__pv_base is views } from "m.malloy"

#@ persist storage="lake"
source: views__v_sibling__view is views__pv_base -> v_sibling

#@ persist { storage="lake" }
source: views__v_nested__view is views__pv_base -> v_nested

-- ==== <pkg>/m.malloy.preaggregate.malloy
##! experimental.persistence
import { rollups__rollup_base is rollups } from "m.malloy"
import { v1_rollups__rollup_base is v1_rollups } from "m.malloy"

#@ persist storage="lake"
source: rollups__rollup__region__495e6697 is rollups__rollup_base -> {
  group_by:
    region
  aggregate:
    r_storage__partial is amount.sum()
}

#@ persist storage="lake"
source: v1_rollups__rollup__region__cbe05f06 is v1_rollups__rollup_base -> {
  group_by:
    region
  aggregate:
    v1_sibling__partial is amount.sum()
}

#@ persist storage="lake"
source: v1_rollups__rollup__status__1425197f is v1_rollups__rollup_base -> {
  group_by:
    status
  aggregate:
    v1_nested__partial is amount.max()
}
