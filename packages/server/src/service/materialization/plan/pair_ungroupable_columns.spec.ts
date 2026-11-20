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
import { PostgresConnection } from "@malloydata/db-postgres";
import {
   FixedConnectionMap,
   getDialect,
   MalloyConfig,
   mkArrayDef,
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

/** The compiled build plan of `model`, over `bq`, `pg` and `duckdb`. */
async function compiled(model: string) {
   const { duckdb } = duckdbTestConnections();
   const connections = new FixedConnectionMap(
      new Map<string, Connection>([
         ["bq", new BigQueryConnection("bq")],
         ["pg", new PostgresConnection("pg")],
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
function pairModel(parent: string, child: string, connection = "bq"): string {
   return `source: trips is ${connection}.sql("select * from ${child}") extend {
  primary_key: trip_id
}
#(materialize) storage="lake" inline=["trips"]
source: s is ${connection}.sql("select * from ${parent}") extend {
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

/**
 * Postgres has no equality operator for `json`, `xml`, the geometric types or
 * a snapshot ("could not identify an equality operator for type json"), so
 * its `GROUP BY` refuses them and arrays of them; `jsonb` groups. Malloy reads
 * each as a `sql native` field, and an array of one as an array of it; the
 * schema below is what Malloy's Postgres connector answers for the columns.
 */
describe("a Postgres pair table", () => {
   const pg = getDialect("postgres");
   const native = (name: string, type: string) => ({
      ...pg.sqlTypeToMalloyType(type),
      name,
   });
   const PG_SCHEMAS: Record<string, unknown[]> = {
      stations: [native("station_id", "integer"), native("name", "text")],
      stations_json: [native("station_id", "integer"), native("attrs", "json")],
      stations_jsonb: [
         native("station_id", "integer"),
         native("attrs", "jsonb"),
      ],
      stations_xml: [native("station_id", "integer"), native("doc", "xml")],
      stations_point: [native("station_id", "integer"), native("loc", "point")],
      stations_json_array: [
         native("station_id", "integer"),
         mkArrayDef(pg.sqlTypeToMalloyType("json"), "attrs"),
      ],
      stations_int_array: [
         native("station_id", "integer"),
         mkArrayDef(pg.sqlTypeToMalloyType("integer"), "codes"),
      ],
      trips: [
         native("trip_id", "integer"),
         native("start_station_id", "integer"),
         native("duration", "integer"),
      ],
   };
   let pgSpy: { mockRestore(): void };
   beforeAll(() => {
      pgSpy = spyOn(
         PostgresConnection.prototype as unknown as {
            fetchSelectSchema(ref: {
               selectStr: string;
               connection: string;
            }): Promise<unknown>;
         },
         "fetchSelectSchema",
      ).mockImplementation(
         async (ref: { selectStr: string; connection: string }) => {
            const table = /from (\w+)/.exec(ref.selectStr)?.[1] ?? "";
            return {
               type: "sql_select",
               ...ref,
               dialect: "postgres",
               name: `pg:${ref.selectStr}`,
               fields: PG_SCHEMAS[table],
            };
         },
      );
   });
   afterAll(() => pgSpy.mockRestore());

   for (const [name, parent] of [
      ["a jsonb column", "stations_jsonb"],
      ["an integer array", "stations_int_array"],
      ["only scalar columns", "stations"],
   ] as const) {
      it(`groups by its stored columns over ${name}`, async () => {
         const c = await compiled(pairModel(parent, "trips", "pg"));
         expect(generatedPlans(c)[0]?.pair?.join).toBe("trips");
      });
   }

   for (const [name, parent, column, type] of [
      ["a json column", "stations_json", "attrs", "JSON"],
      ["an xml column", "stations_xml", "doc", "XML"],
      ["a point column", "stations_point", "loc", "POINT"],
      ["a json array", "stations_json_array", "attrs", "JSON[]"],
   ] as const) {
      it(`is refused at plan over ${name}, which Postgres cannot group by`, async () => {
         const c = await compiled(pairModel(parent, "trips", "pg"));
         expect(generatedPlans(c)).toEqual([]);
         const findings = (c.annotationFindings ?? []).filter(
            (f) => f.code === "materialize-not-built",
         );
         expect(findings.map((f) => f.subject)).toEqual(["s"]);
         expect(findings[0].message).toContain(
            `\`${column}\` is a ${type} column, which postgres`,
         );
      });
   }
});
