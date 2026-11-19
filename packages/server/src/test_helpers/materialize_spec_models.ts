// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The models and helpers the `#(materialize)` planner, build-file generator
 * and stored-chain specs share (`materialize_plan.spec.ts`,
 * `build_file_generator.spec.ts`, `materialize_stored_chain.spec.ts`): a
 * three-row `orders_raw` base, the joined, paired and dated sources beside
 * it, and the plans a compiled package generated.
 */

import type { CompiledPlan, PlannedPackage } from "./materialization_fixture";
import { materializePlansIn } from "./materialization_test_fixtures";
import { buildSourceName } from "../service/materialization/plan/build_file_generator";
import { type MaterializePlan } from "../service/materialization/domain/table_plans";

export const BASE = `source: orders_raw is duckdb.sql("""
  select * from (values
    (1, 'east', 10.5, date '2026-01-01'),
    (2, 'west', 20.0, date '2026-01-02'),
    (3, 'east', 5.25, date '2026-01-03')
  ) as t(id, region, amount, order_date)
""")
`;

/** The compiled contents of a package's `m.malloy`. */
export async function contentsOf(
   pkg: PlannedPackage["pkg"],
): Promise<Record<string, unknown>> {
   const { Model } = await import("../service/model");
   const { runtime, modelURL, importBaseURL } = await Model.getModelRuntime(
      pkg.getPackagePath(),
      "m.malloy",
      pkg.getMalloyConfig(),
   );
   const m = await runtime.loadModel(modelURL, { importBaseURL }).getModel();
   return m._modelDef.contents as Record<string, unknown>;
}

/** Every `#(materialize)` plan a compiled package generated. */
export function generatedPlans(c: CompiledPlan): MaterializePlan[] {
   return Object.values(materializePlansIn(c) ?? {});
}

/** A plan of `orders` with an `id` and a computed `big`, and `overrides`. */
export const examplePlan = (
   overrides: Partial<MaterializePlan>,
): MaterializePlan => ({
   sourceName: "orders",
   sourceID: "orders@file:///pkg/m.malloy",
   buildSourceName: buildSourceName("orders"),
   columns: [
      { dimension: "id", column: "id", computed: false },
      { dimension: "big", column: "big", computed: true },
   ],
   persist: {},
   ...overrides,
});

export const JOINED = `${BASE}
source: regions is duckdb.sql("""
  select * from (values ('east', 'East coast'), ('west', 'West coast')) as t(code, label)
""") extend {
  primary_key: code
  dimension: loud_label is upper(label)
}
source: lines is duckdb.sql("""
  select * from (values (1, 1, 2.0), (2, 1, 3.0), (3, 2, 4.0)) as t(line_id, order_id, qty)
""")
`;

export const PAIRED = `${BASE}
source: lines is duckdb.sql("""
  select * from (values (1, 1, 2.0), (2, 1, 3.0), (3, 2, 4.0), (4, 9, 1.0)) as t(line_id, order_id, qty)
""") extend {
  primary_key: line_id
  dimension: big_line is qty > 2
  measure: total_qty is qty.sum()
}
`;

export const DATED = `${BASE}
source: lines is duckdb.sql("""
  select * from (values
    (1, 1, 2.0, date '2026-01-05'),
    (2, 1, 3.0, date '2026-01-02'),
    (3, 2, 4.0, date '2026-01-01'),
    (4, 9, 1.0, date '2026-01-09')
  ) as t(line_id, order_id, qty, loaded_on)
""") extend {
  primary_key: line_id
}
`;
export const pairSource = (keys: string, extra = "") => `${DATED}
#(materialize) storage="lake" inline=["lines"] refresh="incremental" ${keys}
source: orders is orders_raw extend {
  primary_key: id
  join_many: lines on lines.order_id = id${extra}
}
`;

export const PICKED = `${DATED}
source: picks is duckdb.sql("""
  select * from (values (10, 1, date '2026-01-07'), (11, 3, date '2026-01-08')) as t(pick_id, line_id, picked_on)
""") extend { primary_key: pick_id }
source: lines_picked is lines extend {
  join_many: picks on picks.line_id = line_id
}
`;
export const pathSource = (keys: string) => `${PICKED}
#(materialize) storage="lake" inline=["lp.picks"] refresh="incremental" ${keys}
source: orders is orders_raw extend {
  primary_key: id
  join_many: lp is lines_picked on lp.order_id = id
}
`;
