// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-45: a pair table's build (`inline=` on a join_many) groups by every stored
 * column, and BigQuery refuses `GROUP BY` on a GEOGRAPHY, JSON, ARRAY or
 * STRUCT column ("Grouping by expressions of type GEOGRAPHY is not allowed"),
 * so such a pair table failed every build. It is refused at plan instead, with
 * `inline_unsupported`, and the package loads with a `materialize-not-built`
 * warning naming the column.
 *
 * Compiled for real, through the plan: BigQuery's dialect from a BigQuery
 * connection whose schema probe answers a fixed schema per SQL block (nothing
 * reaches BigQuery).
 */

import { BigQueryConnection } from "@malloydata/db-bigquery";
import {
   FixedConnectionMap,
   MalloyConfig,
   type Connection,
} from "@malloydata/malloy";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import { spyOn } from "bun:test";
import {
   duckdbTestConnections,
   tempBuildPlanPackages,
} from "../../../test_helpers/incremental_test_harness";
import { sourcesIn } from "../../../test_helpers/materialization_test_fixtures";
import { generatedPlans } from "../../../test_helpers/materialize_spec_models";
import { installSettings } from "../../../test_helpers/settings_env";
import { compilePackageBuildPlan } from "./build_plan_compile";

type Column = { name: string; type: string; mode: string; fields?: Column[] };

const scalar = (name: string, type: string): Column => ({
   name,
   type,
   mode: "NULLABLE",
});

/** The schema BigQuery's dry run answers, by the table a SQL block reads. */
const SCHEMAS: Record<string, Column[]> = {
   stations: [scalar("station_id", "INTEGER"), scalar("name", "STRING")],
   stations_geo: [
      scalar("station_id", "INTEGER"),
      scalar("station_geom", "GEOGRAPHY"),
   ],
   stations_json: [scalar("station_id", "INTEGER"), scalar("attrs", "JSON")],
   stations_array: [
      scalar("station_id", "INTEGER"),
      { name: "tags", type: "STRING", mode: "REPEATED" },
   ],
   stations_struct: [
      scalar("station_id", "INTEGER"),
      {
         name: "info",
         type: "RECORD",
         mode: "NULLABLE",
         fields: [scalar("city", "STRING")],
      },
   ],
   trips: [
      scalar("trip_id", "INTEGER"),
      scalar("start_station_id", "INTEGER"),
      scalar("duration", "INTEGER"),
   ],
   trips_geo: [
      scalar("trip_id", "INTEGER"),
      scalar("start_station_id", "INTEGER"),
      scalar("start_geom", "GEOGRAPHY"),
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
         getSQLBlockSchema(ref: { selectStr: string }): Promise<unknown>;
      },
      "getSQLBlockSchema",
   ).mockImplementation(async (ref: { selectStr: string }) => {
      const table = /from (\w+)/.exec(ref.selectStr)?.[1] ?? "";
      return { fields: SCHEMAS[table] };
   });
});
afterAll(() => {
   schemaSpy.mockRestore();
   restore();
});
afterEach(() => packages.cleanup());

/** The compiled build plan of `model`, over `bq` and `duckdb`. */
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

/** A station source over `parent` with a join_many to trips over `child`, inlined. */
function pairModel(parent: string, child: string): string {
   return `source: trips is bq.sql("select * from ${child}") extend {
  primary_key: trip_id
}
#(materialize) storage="lake" inline=["trips"]
source: s is bq.sql("select * from ${parent}") extend {
  primary_key: station_id
  join_many: trips on trips.start_station_id = station_id
  measure: n is count()
}
`;
}

describe("a BigQuery pair table", () => {
   it("groups by its stored columns when BigQuery can group every one", async () => {
      const c = await compiled(pairModel("stations", "trips"));
      const [plan] = generatedPlans(c);
      expect(plan?.pair?.join).toBe("trips");
      const build = Object.values(sourcesIn(c)).find(
         (s) => s.dialectName === "standardsql",
      );
      const sql = await build?.getSQL();
      expect(sql).toContain("GROUP BY");
      expect(sql).toContain("row_number()");
   });

   for (const [name, parent, child, column, type] of [
      [
         "the parent's GEOGRAPHY column",
         "stations_geo",
         "trips",
         "station_geom",
         "GEOGRAPHY",
      ],
      [
         "the child's GEOGRAPHY column",
         "stations",
         "trips_geo",
         "trips__start_geom",
         "GEOGRAPHY",
      ],
      ["a JSON column", "stations_json", "trips", "attrs", "JSON"],
      ["an ARRAY column", "stations_array", "trips", "tags", "ARRAY"],
      ["a STRUCT column", "stations_struct", "trips", "info", "RECORD"],
   ] as const) {
      it(`is refused at plan over ${name}, which BigQuery cannot group by`, async () => {
         const c = await compiled(pairModel(parent, child));
         expect(generatedPlans(c)).toEqual([]);
         expect(
            Object.values(sourcesIn(c)).filter(
               (s) => s.dialectName === "standardsql",
            ),
         ).toEqual([]);
         const findings = (c.annotationFindings ?? []).filter(
            (f) => f.code === "materialize-not-built",
         );
         expect(findings.map((f) => f.subject)).toEqual(["s"]);
         expect(findings[0].message).toContain(
            `\`${column}\` is a ${type} column, which standardsql (BigQuery) cannot group by`,
         );
      });
   }

   it("is still stored without the join_many inlined: the build selects, not groups", async () => {
      const c = await compiled(
         `#(materialize) storage="lake"
source: s is bq.sql("select * from stations_geo") extend {
  primary_key: station_id
  measure: n is count()
}
`,
      );
      expect(generatedPlans(c).map((p) => p.sourceName)).toEqual(["s"]);
      const build = Object.values(sourcesIn(c)).find(
         (s) => s.dialectName === "standardsql",
      );
      expect(await build?.getSQL()).not.toContain("GROUP BY");
   });
});
