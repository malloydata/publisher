// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-07: the columns a `storage=` build writes include a source's array
 * columns. Malloy types an array column (a BigQuery `ARRAY`, a REPEATED
 * field; a DuckDB list) as a source of its own, not an atomic field, and the
 * build's projection kept only atomic fields, so a `#(materialize)` source
 * over `array_agg(...)` stored no list and answered every query live.
 *
 * Compiled for real, through the plan: BigQuery's dialect from a BigQuery
 * connection whose schema probe answers a fixed REPEATED schema (nothing
 * reaches BigQuery), DuckDB's from the in-memory sandbox.
 */

import { BigQueryConnection } from "@malloydata/db-bigquery";
import {
   FixedConnectionMap,
   MalloyConfig,
   type Connection,
   type PersistSource,
} from "@malloydata/malloy";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import { spyOn } from "bun:test";
import {
   duckdbTestConnections,
   tempBuildPlanPackages,
} from "../../../test_helpers/incremental_test_harness";
import {
   materializePlansIn,
   sourcesIn,
} from "../../../test_helpers/materialization_test_fixtures";
import { installSettings } from "../../../test_helpers/settings_env";
import { projectToStoredColumns, storedColumnNames } from "./build_plan";
import { compilePackageBuildPlan } from "./build_plan_compile";

/**
 * BigQuery's dry-run schema for `bq.sql(...)`: scalars, two REPEATED, a
 * repeated RECORD, a RECORD, a RECORD holding a RECORD, and a RECORD holding
 * a REPEATED.
 */
const BIGQUERY_SCHEMA = {
   fields: [
      { name: "start_station_id", type: "INTEGER", mode: "NULLABLE" },
      { name: "v", type: "STRING", mode: "REPEATED" },
      { name: "w", type: "INTEGER", mode: "REPEATED" },
      {
         name: "lr",
         type: "RECORD",
         mode: "REPEATED",
         fields: [
            { name: "a", type: "STRING", mode: "NULLABLE" },
            { name: "n", type: "INTEGER", mode: "NULLABLE" },
         ],
      },
      {
         name: "rec",
         type: "RECORD",
         mode: "NULLABLE",
         fields: [{ name: "a", type: "STRING", mode: "NULLABLE" }],
      },
      {
         name: "nr",
         type: "RECORD",
         mode: "NULLABLE",
         fields: [
            {
               name: "inner",
               type: "RECORD",
               mode: "NULLABLE",
               fields: [{ name: "b", type: "FLOAT", mode: "NULLABLE" }],
            },
         ],
      },
      {
         name: "rl",
         type: "RECORD",
         mode: "NULLABLE",
         fields: [{ name: "tags", type: "STRING", mode: "REPEATED" }],
      },
   ],
};

const packages = tempBuildPlanPackages();
let restore: () => void;
let schemaSpy: { mockRestore(): void };

beforeAll(() => {
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true", PERSIST_STORAGE_MODE: "on" },
   }).restore;
   schemaSpy = spyOn(
      BigQueryConnection.prototype as unknown as {
         getSQLBlockSchema(): Promise<unknown>;
      },
      "getSQLBlockSchema",
   ).mockImplementation(async () => BIGQUERY_SCHEMA);
});
afterAll(() => {
   schemaSpy.mockRestore();
   restore();
});
afterEach(() => packages.cleanup());

/** The compiled plan of `model`, over `bq` and `duckdb`. */
async function compiled(model: string) {
   const { duckdb } = duckdbTestConnections();
   const connections = new FixedConnectionMap(
      new Map<string, Connection>([
         ["bq", new BigQueryConnection("bq")],
         ["duckdb", duckdb],
      ]),
      "duckdb",
   );
   const config = new MalloyConfig({ connections: {} });
   config.wrapConnections(() => connections);
   const pkg = await packages.create(model, { getMalloyConfig: () => config });
   return compilePackageBuildPlan(pkg);
}

/** The planned persist source named `name`, compiled from `model`. */
async function planned(model: string, name: string): Promise<PersistSource> {
   const plan = await compiled(model);
   const source = Object.values(sourcesIn(plan)).find((s) => s.name === name);
   if (!source) throw new Error(`${name} was not planned`);
   return source;
}

describe("the columns a storage= build stores", () => {
   it("include a BigQuery source's ARRAY and STRUCT columns, nested or holding a list", async () => {
      const source = await planned(
         `#(materialize) storage="lake"
source: s_arr is bq.sql("select start_station_id, array_agg(distinct subscriber_type) as v from t group by 1") extend {
  measure: n is count()
}
`,
         "s_arr",
      );
      expect(source.dialectName).toBe("standardsql");
      expect(storedColumnNames(source)).toEqual([
         "start_station_id",
         "v",
         "w",
         "lr",
         "rec",
         "nr",
         "rl",
      ]);
      // The projection the CTAS reads, quoted for BigQuery.
      const sql = projectToStoredColumns(source, "SELECT 1");
      expect(sql.slice(0, sql.indexOf(" FROM ("))).toBe(
         "SELECT `start_station_id`, `v`, `w`, `lr`, `rec`, `nr`, `rl`",
      );
   });

   it("include a DuckDB source's list and struct columns", async () => {
      const source = await planned(
         `#(materialize) storage="lake"
source: d_arr is duckdb.sql("select 1 as sid, ['a', 'b'] as v, [{'a': 'p'}] as lr, {'a': 'q'} as rec, {'i': {'b': 1.5}} as nr, {'tags': ['t']} as rl") extend {
  measure: n is count()
}
`,
         "d_arr",
      );
      expect(storedColumnNames(source)).toEqual([
         "sid",
         "v",
         "lr",
         "rec",
         "nr",
         "rl",
      ]);
   });

   it("make a #(materialize) source's struct columns plan columns, not a computed record", async () => {
      const model = `#(materialize) storage="lake"
source: d_rec is duckdb.sql("select 1 as sid, {'a': 'q', 'n': 2} as rec") extend {
  dimension: made is { a is 'x' }
  measure: n is count()
}
`;
      const plan = Object.values(materializePlansIn(await compiled(model)))[0];
      expect(plan.columns).toContainEqual(
         expect.objectContaining({ column: "rec", computed: false }),
      );
      expect(plan.columns.map((c) => c.column)).not.toContain("made");
      expect(storedColumnNames(await planned(model, "d_rec"))).toEqual([
         "sid",
         "rec",
      ]);
   });

   it("include a hidden struct, marked with its access", async () => {
      const model = `##! experimental { access_modifiers }
#(materialize) storage="lake"
source: hr is duckdb.sql("select 1 as sid, {'a': 'q', 'n': 2} as rec") include {
  internal: rec
  public: *
} extend {
  measure: total is rec.n.sum()
}
`;
      const plan = Object.values(materializePlansIn(await compiled(model)))[0];
      expect(plan.columns).toContainEqual(
         expect.objectContaining({ column: "rec", access: "internal" }),
      );
      expect(storedColumnNames(await planned(model, "hr"))).toContain("rec");
   });

   it("include a v1 persisted query's nest and the lists it selects", async () => {
      const model = `##! experimental.persistence
source: raw is duckdb.sql("select 1 as sid, 'x' as org, ['a', 'b'] as v")
#@ persist storage="lake"
source: p_nest is raw -> {
  group_by: org
  nest: by_sid is { group_by: sid }
}
#@ persist storage="lake"
source: p_list is raw -> { select: sid, v }
`;
      expect(storedColumnNames(await planned(model, "p_nest"))).toEqual([
         "org",
         "by_sid",
      ]);
      expect(storedColumnNames(await planned(model, "p_list"))).toEqual([
         "sid",
         "v",
      ]);
   });

   it("include a hidden list, as a hidden dimension is, marked with its access", async () => {
      const model = `##! experimental { access_modifiers }
#(materialize) storage="lake"
source: h is duckdb.sql("select 1 as sid, ['a', 'b'] as v, 2 as amount") include {
  private: v, amount
  public: *
} extend {
  measure: nv is count(v.each)
}
`;
      const plan = Object.values(materializePlansIn(await compiled(model)))[0];
      expect(plan.columns).toContainEqual(
         expect.objectContaining({ column: "v", access: "private" }),
      );
      expect(plan.columns).toContainEqual(
         expect.objectContaining({ column: "amount", access: "private" }),
      );
      expect(storedColumnNames(await planned(model, "h"))).toContain("v");
   });
});
