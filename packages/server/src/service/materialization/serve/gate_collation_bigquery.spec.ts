// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A BigQuery source's string comparison, judged at the probe boundary: a
 * BigQuery connection compiled for real, whose schema reads and SQL are
 * stubbed (no warehouse). A column with a collation (`und:ci`, written on
 * the column, or resolved into it from a table's or a dataset's default)
 * compares case-insensitively live and byte-exact in a `storage=` copy.
 *
 * The project is read first, in the connection's location: one with no
 * collated column answers for every table in it, an `.sql()` source's too.
 * Only a dataset it lists is read table by table. A COLLATE in the query's
 * SQL, a failed or denied read, and author SQL that may read a collated
 * project each answer live. Untested against a real BigQuery.
 */

import { BigQueryConnection } from "@malloydata/db-bigquery";
import {
   type Connection,
   type QueryMaterializer,
   SingleConnectionRuntime,
} from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import {
   storedTableCollationRefusal,
   stringGateCollationRefusal,
} from "./gate_collation";
import type { GateReads } from "./gate_reads_types";
import { resultReadsOfQuery } from "./result_reads";

/** What the stub's catalog answers. */
interface Catalog {
   location?: string;
   /**
    * The location `datasets.list` names for every dataset of a project;
    * absent, the client cannot be reached.
    */
   listed?: string;
   /** Each project's datasets that hold a collated column, or a failure. */
   projects?: Record<string, string[] | "fail">;
   /** Datasets a project-wide read does not see (no permission, say). */
   unseen?: string[];
   /** Each dataset's collated columns, by table, then path; or a failure. */
   datasets?: Record<string, Record<string, Record<string, string>> | "fail">;
   /** Tables a dataset's read does not see. */
   unseenTables?: string[];
}

/** Every dataset and table the model names, as the stub's catalog holds them. */
const TABLES: Record<string, string[]> = {
   "proj.ds": ["plain_t", "ci_t", "copy_t"],
   ds2: ["other_t"],
};

/**
 * A BigQuery connection (project `proj`) whose table schemas are
 * `(id, region)`, and whose `INFORMATION_SCHEMA` reads answer `catalog`,
 * recording each SQL it was sent and each region read.
 */
function stubBigQuery(catalog: Catalog): {
   connection: Connection;
   sent: string[];
   regions: string[];
} {
   const connection = new BigQueryConnection({
      name: "bq",
      projectId: "proj",
      billingProjectId: "proj",
      ...(catalog.location ? { location: catalog.location } : {}),
   });
   const sent: string[] = [];
   const regions: string[] = [];
   const stub = connection as unknown as Record<string, unknown>;
   stub.bigQuery = {
      getDatasets: async () => {
         if (catalog.listed === undefined) throw new Error("no credentials");
         return [[{ metadata: { location: catalog.listed } }]];
      },
   };
   const fields = [
      { name: "id", type: "number" },
      { name: "region", type: "string" },
   ];
   stub.fetchTableSchema = async (tableName: string, tablePath: string) => ({
      type: "table",
      name: tableName,
      tablePath,
      connection: "bq",
      dialect: "standardsql",
      fields,
   });
   stub.fetchSelectSchema = async (sqlRef: { selectStr: string }) => ({
      type: "sql_select",
      name: "s",
      selectStr: sqlRef.selectStr,
      connection: "bq",
      dialect: "standardsql",
      fields,
   });
   stub.runSQL = async (sql: string) => {
      sent.push(sql);
      const region =
         /FROM `([^`]+)`\.`region-([^`]+)`\.INFORMATION_SCHEMA/.exec(sql);
      if (region) {
         regions.push(region[2]);
         const collated = catalog.projects?.[region[1]] ?? [];
         if (collated === "fail") throw new Error("Access Denied");
         const seen = new Set([
            ...(region[1] === "proj" ? ["ds", "ds2"] : ["ds"]),
            ...collated,
         ]);
         for (const d of catalog.unseen ?? []) seen.delete(d);
         return {
            rows: [...seen].map((d) => ({
               table_schema: d,
               collated: collated.includes(d),
            })),
            totalRows: 0,
         };
      }
      const dataset = /FROM `([^`]+)`(?:\.`([^`]+)`)?\.INFORMATION_SCHEMA/.exec(
         sql,
      );
      if (!dataset) throw new Error(`unexpected SQL: ${sql}`);
      const name = dataset[2] ? `${dataset[1]}.${dataset[2]}` : dataset[1];
      const tables = catalog.datasets?.[name] ?? {};
      if (tables === "fail") throw new Error("Access Denied");
      const names = new Set([...(TABLES[name] ?? []), ...Object.keys(tables)]);
      for (const t of catalog.unseenTables ?? []) names.delete(t);
      return {
         rows: [...names].flatMap((table) => [
            { table_name: table, path: "", collation_name: "" },
            ...Object.entries(tables[table] ?? {}).map(([path, c]) => ({
               table_name: table,
               path,
               collation_name: c,
            })),
         ]),
         totalRows: 0,
      };
   };
   return { connection: connection as unknown as Connection, sent, regions };
}

const MODEL = `##! experimental { sql_functions }
source: plain is bq.table('proj.ds.plain_t')
source: ci is bq.table('proj.ds.ci_t')
source: other is bq.table('ds2.other_t')
source: joined is bq.table('proj.ds.plain_t') extend {
  join_one: o is bq.table('ds2.other_t') on o.id = id
}
source: authored is bq.sql("SELECT 1 AS id, 'a' AS region FROM proj.ds.plain_t")
source: elsewhere is bq.sql("SELECT id, region FROM \`other-project.ds.t\`")
source: plain_sql is plain extend {
  dimension: r is sql_string("UPPER(\${region})")
}
`;

/** `ci_t` in `proj.ds` holds an und:ci column. */
const CI_DS = { ci_t: { region: "und:ci" } };

/** The refusal a BigQuery query under a collation, or unread, answers. */
const LIVE = "a query that reads a string under a collation";

/** Reads that compare a string, as a gate's or a query's filter's would. */
const STRING_READS: GateReads = {
   names: new Set(["region"]),
   columns: [["region"]],
   unresolved: false,
   scoped: true,
   gated: false,
   equalityOnly: false,
   entryPaths: [["region"]],
   entryPathsComplete: true,
   comparesNoString: false,
};

function runnable(connection: Connection, query: string): QueryMaterializer {
   return new SingleConnectionRuntime({ connection })
      .loadModel(MODEL)
      .loadQuery(query);
}

async function refusal(
   catalog: Catalog,
   query: string,
): Promise<{ why: string | undefined; sent: string[]; regions: string[] }> {
   const { connection, sent, regions } = stubBigQuery(catalog);
   const why = await stringGateCollationRefusal({
      authorRunnable: runnable(connection, query),
      reads: STRING_READS,
   });
   return { why, sent, regions };
}

describe("a BigQuery source's string comparison, with a location", () => {
   it("is binary when the project holds no collated column, one read", async () => {
      const { why, sent } = await refusal(
         { location: "US" },
         "run: plain -> { group_by: region; order_by: region }",
      );
      expect(why).toBeUndefined();
      expect(sent).toHaveLength(1);
      expect(sent[0]).toContain(
         "`proj`.`region-us`.INFORMATION_SCHEMA.COLUMNS",
      );
      expect(sent[0]).toContain("COLUMN_FIELD_PATHS");
   });

   it("serves an .sql() source when its project holds no collated column", async () => {
      const { why, sent } = await refusal(
         { location: "US" },
         "run: authored -> { group_by: region }",
      );
      expect(why).toBeUndefined();
      expect(sent).toHaveLength(1);
   });

   it("answers an .sql() source live when its project holds a collated column", async () => {
      const { why } = await refusal(
         { location: "US", projects: { proj: ["ds"] } },
         "run: authored -> { group_by: region }",
      );
      expect(why).toBe(LIVE);
   });

   it("checks another project an .sql() source names, and answers live when it cannot", async () => {
      const ok = await refusal(
         { location: "US" },
         "run: elsewhere -> { group_by: region }",
      );
      expect(ok.why).toBeUndefined();
      expect(
         ok.sent.some((s) => s.includes("`other-project`.`region-us`")),
      ).toBe(true);
      const denied = await refusal(
         { location: "US", projects: { "other-project": "fail" } },
         "run: elsewhere -> { group_by: region }",
      );
      expect(denied.why).toBe(LIVE);
   });

   it("reads a listed dataset table by table: a collated table live, another served", async () => {
      const catalog: Catalog = {
         location: "US",
         projects: { proj: ["ds"] },
         datasets: { "proj.ds": CI_DS },
      };
      const ci = await refusal(
         catalog,
         "run: ci -> { group_by: region; order_by: region }",
      );
      expect(ci.why).toBe(LIVE);
      expect(ci.sent).toHaveLength(2);
      expect(ci.sent[1]).toContain("`proj`.`ds`.INFORMATION_SCHEMA.COLUMNS");
      const plain = await refusal(
         catalog,
         "run: plain -> { group_by: region; order_by: region }",
      );
      expect(plain.why).toBeUndefined();
   });

   it("does not read a dataset the project does not list", async () => {
      const { why, sent } = await refusal(
         {
            location: "US",
            projects: { proj: ["elsewhere_ds"] },
            datasets: { "proj.ds": CI_DS },
         },
         "run: ci -> { group_by: region }",
      );
      expect(why).toBeUndefined();
      expect(sent).toHaveLength(1);
   });

   it("answers live when the project read fails", async () => {
      const { why } = await refusal(
         { location: "US", projects: { proj: "fail" } },
         "run: plain -> { group_by: region }",
      );
      expect(why).toBe(LIVE);
   });

   it("answers live when a listed dataset's read fails", async () => {
      const { why } = await refusal(
         {
            location: "US",
            projects: { proj: ["ds"] },
            datasets: { "proj.ds": "fail" },
         },
         "run: plain -> { group_by: region }",
      );
      expect(why).toBe(LIVE);
   });

   it("answers live when the query's SQL names a COLLATE", async () => {
      // The text check, as on DuckDB: any COLLATE in the query's SQL, here
      // only a literal's, over a project that holds no collated column.
      const { why, sent } = await refusal(
         { location: "US" },
         "run: plain -> { where: region = 'a COLLATE b'; group_by: region }",
      );
      expect(why).toBe(LIVE);
      expect(sent).toHaveLength(0);
   });

   it("reads each project once per loaded model, and again after a failure", async () => {
      const catalog: Catalog = { location: "US", projects: { proj: "fail" } };
      const { connection, sent } = stubBigQuery(catalog);
      const model = new SingleConnectionRuntime({ connection }).loadModel(
         MODEL,
      );
      const ask = (q: string) =>
         stringGateCollationRefusal({
            authorRunnable: model.loadQuery(q),
            reads: STRING_READS,
         });
      expect(await ask("run: plain -> { group_by: region }")).toBe(LIVE);
      catalog.projects = {};
      expect(await ask("run: plain -> { group_by: region }")).toBeUndefined();
      expect(await ask("run: ci -> { group_by: region }")).toBeUndefined();
      expect(sent).toHaveLength(2);
   });
});

describe("a BigQuery source's coverage", () => {
   it("answers live when no project-wide read saw the table's dataset", async () => {
      const { why } = await refusal(
         { location: "US", unseen: ["ds"] },
         "run: plain -> { group_by: region }",
      );
      expect(why).toBe(LIVE);
   });

   it("answers live when the dataset's read did not see the table", async () => {
      const { why } = await refusal(
         {
            location: "US",
            projects: { proj: ["ds"] },
            datasets: { "proj.ds": CI_DS },
            unseenTables: ["plain_t"],
         },
         "run: plain -> { group_by: region }",
      );
      expect(why).toBe(LIVE);
   });
});

describe("a BigQuery source's string comparison, with no location set", () => {
   it("reads each region datasets.list names, an .sql() source served", async () => {
      const { why, regions } = await refusal(
         { listed: "EU" },
         "run: authored -> { group_by: region }",
      );
      expect(why).toBeUndefined();
      expect(regions).toEqual(["eu"]);
   });

   it("answers an .sql() source live when a derived region lists a collated dataset", async () => {
      const { why } = await refusal(
         { listed: "EU", projects: { proj: ["ds"] } },
         "run: authored -> { group_by: region }",
      );
      expect(why).toBe(LIVE);
   });
});

describe("a BigQuery source's string comparison, with no location and no client", () => {
   it("reads each dataset a .table() source reads", async () => {
      const catalog: Catalog = { datasets: { "proj.ds": CI_DS } };
      const plain = await refusal(
         catalog,
         "run: plain -> { group_by: region }",
      );
      expect(plain.why).toBeUndefined();
      expect(plain.sent).toHaveLength(1);
      expect(plain.sent[0]).toContain("`proj`.`ds`.INFORMATION_SCHEMA");
      const ci = await refusal(catalog, "run: ci -> { group_by: region }");
      expect(ci.why).toBe(LIVE);
   });

   it("answers live when a joined table in another dataset holds one", async () => {
      const { why } = await refusal(
         { datasets: { ds2: { other_t: { region: "und:ci" } } } },
         "run: joined -> { group_by: o.region }",
      );
      expect(why).toBe(LIVE);
   });

   it("answers an .sql() source or a raw SQL expression live: no project to read", async () => {
      for (const q of [
         "run: authored -> { group_by: region }",
         "run: plain_sql -> { group_by: r }",
      ]) {
         const { why, sent } = await refusal({}, q);
         expect(why).toBe(LIVE);
         expect(sent).toHaveLength(0);
      }
   });
});

describe("a BigQuery result's re-grouping and ordering", () => {
   it("is judged by the same probe, as the result rule reads it", async () => {
      // A rollup answering a coarser group_by, or the served copy ordering a
      // string, re-groups and orders the copy's bytes: the result rule asks
      // the source-side probe, which runs only for a dialect whose
      // collation is read.
      const { connection } = stubBigQuery({
         location: "US",
         projects: { proj: ["ds"] },
         datasets: { "proj.ds": CI_DS },
      });
      const q = "run: ci -> { group_by: region; order_by: region }";
      const reads = await resultReadsOfQuery(
         runnable(connection, q),
         new Set(),
         { fromStage: 0 },
      );
      expect(reads?.stringCollation).toBe("dataset");
      expect(reads?.grouped.comparesNoString).toBe(false);
      expect(
         await stringGateCollationRefusal({
            authorRunnable: runnable(connection, q),
            reads: reads?.grouped,
         }),
      ).toBe(LIVE);
   });
});

describe("a BigQuery database table", () => {
   const ask = (catalog: Catalog, table: string, query: string) => {
      const { connection } = stubBigQuery(catalog);
      const r = runnable(connection, query);
      return storedTableCollationRefusal({
         tables: [{ tier: "database", tablePath: table }],
         answerRunnable: r,
         virtualMap: undefined,
         authorRunnable: r,
         reads: STRING_READS,
      });
   };

   it("is kept when its source reads no collated string", async () => {
      expect(
         await ask(
            { location: "US" },
            "proj.ds.copy_t",
            "run: plain -> { group_by: region }",
         ),
      ).toBeUndefined();
   });

   it("answers live when its source's column is collated and its own is not", async () => {
      expect(
         await ask(
            {
               location: "US",
               projects: { proj: ["ds"] },
               datasets: { "proj.ds": CI_DS },
            },
            "proj.ds.copy_t",
            "run: ci -> { group_by: region }",
         ),
      ).toBeDefined();
   });

   it("is kept when its column reads back the source's collation", async () => {
      expect(
         await ask(
            {
               location: "US",
               projects: { proj: ["ds"] },
               datasets: {
                  "proj.ds": { ...CI_DS, copy_t: { region: "und:ci" } },
               },
            },
            "proj.ds.copy_t",
            "run: ci -> { group_by: region }",
         ),
      ).toBeUndefined();
   });

   it("answers live when its column reads back another collation", async () => {
      expect(
         await ask(
            {
               location: "US",
               projects: { proj: ["ds"] },
               datasets: {
                  "proj.ds": { ...CI_DS, copy_t: { region: "und:cs" } },
               },
            },
            "proj.ds.copy_t",
            "run: ci -> { group_by: region }",
         ),
      ).toBeDefined();
   });
});
