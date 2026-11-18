// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The G3 models: sources under their own row-level gate (an
 * `#(access_filter)` row filter, an `#(authorize)` lock, both, or inherited),
 * stored in the database tier and in a `storage=` destination (g3_gates, once per
 * tier). Shared with the served copy's worker parity spec, which compares
 * the gates' grafts on both threads.
 */

// Each materialized source selects its own columns, so no two share a
// content address (a shared table is refused when any sharer is).
export const G3_DATABASE_MODEL = `##! experimental { givens }
import "raw.malloy"

given: REGIONS :: string[]
given: ROLE :: string
given: STATUS :: string is 'placed'

#(access_filter) region in $REGIONS
#(materialize) name="g3_gated"
source: gated is orders_raw -> {
  select: id, customer_id, region, status, amount
} extend {
  measure: total is amount.sum(), n is count()
}

source: gated_ext is gated extend {
  measure: avg_amount is amount.avg()
}

#(materialize) name="g3_ext_tbl"
source: gated_ext_tbl is gated extend { dimension: big is amount > 100 }

source: joiner is customers_raw extend {
  join_many: g is gated on g.customer_id = id
  measure: n_customers is count()
}

#(materialize) name="g3_join_tbl"
source: mat_joiner is customers_raw extend {
  join_many: g is gated on g.customer_id = id
  measure: n_customers is count()
}

#(authorize) $ROLE = 'admin'
#(materialize) name="g3_lock"
source: locked is orders_raw -> { select: id, region } extend {
  measure: n is count()
}

#(authorize) $ROLE = 'admin'
#(access_filter) region in $REGIONS
#(materialize) name="g3_lock_rows"
source: locked_rows is orders_raw -> { select: id, region, status } extend {
  measure: n is count()
}

#(access_filter) region in $REGIONS
#(materialize) name="g3_pairs" inline=["lines"]
source: gated_pairs is orders_raw -> {
  select: id, region, customer_id
} extend {
  primary_key: id
  join_many: lines is lines_raw on lines.order_id = id
  measure: n is count(), qty is lines.qty.sum()
}

#(access_filter) region in $REGIONS
#(materialize) name="g3_inline" inline=["c"]
source: gated_inline is orders_raw -> {
  select: id, region, customer_id, status
} extend {
  primary_key: id
  join_one: c is customers_raw on c.id = customer_id
  measure: n is count()
}

#(access_filter) region in $REGIONS
#(materialize) storage="lake"
source: gated_sto is orders_raw -> { select: id, region, amount } extend {
  measure: n is count()
}

#(access_filter) region in $REGIONS
#(materialize) name="g3_baked"
source: baked is orders_raw -> {
  where: status = $STATUS
  select: id, region, status, customer_id
} extend {
  measure: n is count()
}
`;

// Each stored source selects its own columns, so no two share a content
// address (a shared table is refused when any sharer is). Every `mz_` source
// is `#(materialize)`, every `v1_` source `#@ persist`, all `storage=`.
export const G3_STORAGE_MODEL = `##! experimental { givens persistence }
import "raw.malloy"

given: REGIONS :: string[]
given: ROLE :: string
given: STATUS :: string is 'placed'

#(access_filter) region in $REGIONS
#(materialize) name="g3s_mz" storage="lake"
source: mz_gated is orders_raw -> {
  select: id, customer_id, region, status, amount
} extend {
  measure: total is amount.sum(), n is count()
}

#(access_filter) region in $REGIONS
#@ persist name="g3s_v1" storage="lake"
source: v1_gated is orders_raw -> {
  select: id, customer_id, region, amount
} extend {
  measure: total is amount.sum(), n is count()
}

source: mz_ext is mz_gated extend {
  measure: avg_amount is amount.avg()
}

source: v1_ext is v1_gated extend {
  measure: avg_amount is amount.avg()
}

#(authorize) $ROLE = 'admin'
#(materialize) name="g3s_mz_lock" storage="lake"
source: mz_lock is orders_raw -> { select: id, region } extend {
  measure: n is count()
}

#(authorize) $ROLE = 'admin'
#@ persist name="g3s_v1_lock" storage="lake"
source: v1_lock is orders_raw -> { select: id, region, status } extend {
  measure: n is count()
}

#(authorize) $ROLE = 'admin'
#(access_filter) region in $REGIONS
#(materialize) name="g3s_mz_lr" storage="lake"
source: mz_lock_rows is orders_raw -> {
  select: id, region, status, customer_id
} extend {
  measure: n is count()
}

#(authorize) $ROLE = 'admin'
#(access_filter) region in $REGIONS
#@ persist name="g3s_v1_lr" storage="lake"
source: v1_lock_rows is orders_raw -> { select: id, region, customer_id } extend {
  measure: n is count()
}

#(authorize) $ROLE = 'admin'
source: locked_base is orders_raw extend {}

#(materialize) name="g3s_mz_inh_lock" storage="lake"
source: mz_inh_lock is locked_base -> { select: id, region, amount } extend {
  measure: n is count()
}

#@ persist name="g3s_v1_inh_lock" storage="lake"
source: v1_inh_lock is locked_base -> {
  select: id, region, status, amount
} extend {
  measure: n is count()
}

#(access_filter) region in $REGIONS
source: rows_base is orders_raw extend {}

#(materialize) name="g3s_mz_inh_rows" storage="lake"
source: mz_inh_rows is rows_base -> { select: id, region, discount } extend {
  measure: n is count()
}

#@ persist name="g3s_v1_inh_rows" storage="lake"
source: v1_inh_rows is rows_base -> {
  select: id, region, status, discount
} extend {
  measure: n is count()
}

#(materialize) name="g3s_mz_joiner" storage="lake"
source: mz_joiner is customers_raw extend {
  join_many: g is mz_gated on g.customer_id = id
  measure: n_customers is count()
}

#(access_filter) region in $REGIONS
#(materialize) name="g3s_mz_baked" storage="lake"
source: mz_baked is orders_raw -> {
  where: status = $STATUS
  select: id, region, status, discount, amount
} extend {
  measure: n is count()
}

#(access_filter) region in $REGIONS
#@ persist name="g3s_v1_baked" storage="lake"
source: v1_baked is orders_raw -> {
  where: status = $STATUS
  select: id, region, customer_id, discount
} extend {
  measure: n is count()
}

#(materialize) name="g3s_cust" storage="lake"
source: cust_sto is customers_raw -> { select: id, segment } extend {
  primary_key: id
}

#(access_filter) region in $REGIONS
#(materialize) name="g3s_mz_mix" storage="lake"
source: mz_mix is orders_raw -> {
  select: id, customer_id, region, amount, discount
} extend {
  join_one: c is cust_sto on c.id = customer_id
  measure: n is count()
}

#(access_filter) region in $REGIONS
#@ persist name="g3s_v1_mix" storage="lake"
source: v1_mix is orders_raw -> {
  select: id, customer_id, region, discount
} extend {
  join_one: c is cust_sto on c.id = customer_id
  measure: n is count()
}
`;
