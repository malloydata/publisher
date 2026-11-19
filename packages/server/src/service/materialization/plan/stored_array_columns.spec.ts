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
import { sourcesIn } from "../../../test_helpers/materialization_test_fixtures";
import { installSettings } from "../../../test_helpers/settings_env";
import { projectToStoredColumns, storedColumnNames } from "./build_plan";
import { compilePackageBuildPlan } from "./build_plan_compile";

/** BigQuery's dry-run schema for `bq.sql(...)`: scalars, two REPEATED, a RECORD. */
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

/** The planned persist source named `name`, compiled from `model`. */
async function planned(model: string, name: string): Promise<PersistSource> {
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
   const plan = await compilePackageBuildPlan(pkg);
   const source = Object.values(sourcesIn(plan)).find((s) => s.name === name);
   if (!source) throw new Error(`${name} was not planned`);
   return source;
}

describe("the columns a storage= build stores", () => {
   it("include a BigQuery source's ARRAY columns, scalar or record, but not a record", async () => {
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
      ]);
      // The projection the CTAS reads, quoted for BigQuery.
      const sql = projectToStoredColumns(source, "SELECT 1");
      expect(sql.slice(0, sql.indexOf(" FROM ("))).toBe(
         "SELECT `start_station_id`, `v`, `w`, `lr`",
      );
   });

   it("include a DuckDB source's list columns", async () => {
      const source = await planned(
         `#(materialize) storage="lake"
source: d_arr is duckdb.sql("select 1 as sid, ['a', 'b'] as v, [{'a': 'p'}] as lr, {'a': 'q'} as rec") extend {
  measure: n is count()
}
`,
         "d_arr",
      );
      expect(storedColumnNames(source)).toEqual(["sid", "v", "lr"]);
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
});
