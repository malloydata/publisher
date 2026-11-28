// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A BigQuery source's string comparison, judged at the probe boundary: a
 * BigQuery connection compiled for real, whose schema reads and SQL are
 * stubbed (no warehouse). A column with a collation (`und:ci`, written on
 * the column, or resolved into it from a table's or a dataset's default)
 * compares case-insensitively live and byte-exact in a `storage=` copy.
 *
 * A model whose tables are all known (`.table()` sources) in a few datasets
 * reads each dataset's own catalog, as a dataset's READER may. An `.sql()`
 * source, and a view, are proved by the project-wide read in the
 * connection's location: no collated column anywhere in the project. Past
 * the dataset bound, the project is read first and only a dataset it lists
 * is read table by table. A COLLATE in the query's SQL, a failed or denied
 * read, and author SQL that may read a collated project each answer live.
 * Untested against a real BigQuery.
 */

import { BigQueryConnection } from "@malloydata/db-bigquery";
import {
   type Connection,
   type QueryMaterializer,
   SingleConnectionRuntime,
} from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { bigqueryBuildCompareBinary } from "./bigquery_build";
import type { GateReads } from "../gate_reads_types";
import { resultReadsOfQuery } from "../result_reads";
import { lookupSourceRefusal, lookupStoredRefusal } from "../admission_lookup";
import { makeSnapshot } from "../../domain/binding";
import { parseContract } from "../../domain/column_contract";
import { captureDatabaseContract } from "../../run/column_contract_capture";
import { admitRead, type ReadFacts } from "../../domain/admission";
import type { TableRead } from "../../domain/gate_policy";
import {
   readStringCapabilities,
   recordStringCapabilities,
} from "../source_capability";

/** What the stub's catalog answers. */
interface Catalog {
   location?: string;
   /**
    * The location `datasets.list` names for every dataset of a project, or
    * "none" when it lists no dataset; absent, the client cannot be reached.
    */
   listed?: string;
   /** Each project's datasets that hold a collated column, or a failure. */
   projects?: Record<string, string[] | "fail">;
   /** Tables (`dataset.table`) a project-wide read does not see. */
   unseen?: string[];
   /** Tables (`dataset.table`) a dataset's own read does not see. */
   unseenInDataset?: string[];
   /** Each table's type, by `dataset.table`; absent, a base table. */
   types?: Record<string, string>;
   /** Each dataset's collated columns, by table, then path; or a failure. */
   datasets?: Record<string, Record<string, Record<string, string>> | "fail">;
   /** Every table's schema; absent, `(id, region)`. */
   fields?: { name: string; type: string }[];
}

/** Every table the model names, by project, then dataset. */
const TABLES: Record<string, Record<string, string[]>> = {
   proj: {
      ds: ["plain_t", "ci_t", "copy_t"],
      ds2: ["other_t"],
      ecomm_e2e: ["users"],
   },
   "other-project": { ds: ["t"] },
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
         return [
            catalog.listed === "none"
               ? []
               : [{ metadata: { location: catalog.listed } }],
         ];
      },
   };
   const fields = catalog.fields ?? [
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
   const typeOf = (key: string) => catalog.types?.[key] ?? "BASE TABLE";
   const answer = async (
      sql: string,
   ): Promise<{ rows: Record<string, unknown>[] }> => {
      sent.push(sql);
      const region =
         /FROM `([^`]+)`\.`region-([^`]+)`\.INFORMATION_SCHEMA/.exec(sql);
      if (region) {
         regions.push(region[2]);
         const project = region[1];
         const collated = catalog.projects?.[project] ?? [];
         if (collated === "fail") throw new Error("Access Denied");
         if (sql.includes(".TABLES")) {
            const wanted = [...sql.matchAll(/'([^']+)'/g)].map((m) => m[1]);
            const held = Object.entries(TABLES[project] ?? {}).flatMap(
               ([d, ts]) => ts.map((t) => `${d}.${t}`),
            );
            return {
               rows: wanted
                  .filter((w) => held.includes(w))
                  .filter((w) => !(catalog.unseen ?? []).includes(w))
                  .map((w) => {
                     const [d, t] = w.split(".");
                     return {
                        table_schema: d,
                        table_name: t,
                        table_type: typeOf(w),
                     };
                  }),
            };
         }
         return {
            rows: collated.map((d) => ({ table_schema: d })),
         };
      }
      const dataset = /FROM `([^`]+)`(?:\.`([^`]+)`)?\.INFORMATION_SCHEMA/.exec(
         sql,
      );
      if (!dataset) throw new Error(`unexpected SQL: ${sql}`);
      const name = dataset[2] ? `${dataset[1]}.${dataset[2]}` : dataset[1];
      const cols = catalog.datasets?.[name] ?? {};
      if (cols === "fail") throw new Error("Access Denied");
      const project = dataset[2] ? dataset[1] : "proj";
      const datasetName = dataset[2] ?? dataset[1];
      const names = (TABLES[project]?.[datasetName] ?? []).filter(
         (t) =>
            !(catalog.unseenInDataset ?? []).includes(`${datasetName}.${t}`),
      );
      return {
         rows: names.flatMap((table) => [
            {
               table_name: table,
               table_type: typeOf(`${datasetName}.${table}`),
               path: "",
               collation_name: "",
            },
            ...Object.entries(cols[table] ?? {}).map(([path, c]) => ({
               table_name: table,
               table_type: "",
               path,
               collation_name: c,
            })),
         ]),
      };
   };
   // One whole page: every row the read holds, as many as `totalRows` says.
   stub.runSQL = async (sql: string) => {
      const { rows } = await answer(sql);
      return { rows, totalRows: rows.length };
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
source: wild is bq.table('x.proj.ds.plain_t')
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
   reads: GateReads | undefined = STRING_READS,
   /** The bind's dataset bound; 0 reads every table project-wide first. */
   datasetReadLimit?: number,
): Promise<{ why: string | undefined; sent: string[]; regions: string[] }> {
   // Looked up over what a bind records of the catalog.
   const { connection, sent, regions } = stubBigQuery(catalog);
   const runtime = new SingleConnectionRuntime({ connection });
   const materializer = runtime.loadModel(MODEL);
   recordStringCapabilities(
      runtime,
      await readStringCapabilities({
         modelDef: (await materializer.getModel())._modelDef,
         load: (q) => materializer.loadQuery(q),
         connection: async () => connection,
         ...(datasetReadLimit !== undefined ? { datasetReadLimit } : {}),
      }),
   );
   const why = await lookupSourceRefusal({
      authorRunnable: materializer.loadQuery(query),
      reads,
   });
   return { why, sent, regions };
}

describe("a BigQuery source's string comparison, with a location", () => {
   it("is binary when its dataset's own read sees the table with no collated column", async () => {
      const { why, sent } = await refusal(
         { location: "US" },
         "run: plain -> { group_by: region; order_by: region }",
      );
      expect(why).toBeUndefined();
      expect(sent.join("\n")).toContain(
         "`proj`.`ds`.INFORMATION_SCHEMA.TABLES",
      );
      // The tables' types come from their datasets' own reads; the project
      // is read only for the model's `.sql()` sources.
      expect(sent.join("\n")).not.toContain(
         "`proj`.`region-us`.INFORMATION_SCHEMA.TABLES",
      );
      expect(sent.join("\n")).toContain(
         "`proj`.`region-us`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS",
      );
   });

   it("past the dataset bound, is binary when the project holds no collated column and its read sees the table", async () => {
      const { why, sent } = await refusal(
         { location: "US" },
         "run: plain -> { group_by: region; order_by: region }",
         STRING_READS,
         0,
      );
      expect(why).toBeUndefined();
      expect(sent.join("\n")).toContain(
         "`proj`.`region-us`.INFORMATION_SCHEMA.TABLES",
      );
   });

   it("serves an .sql() source when its project holds no collated column", async () => {
      const { why } = await refusal(
         { location: "US" },
         "run: authored -> { group_by: region }",
      );
      expect(why).toBeUndefined();
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

   it("reads a collated dataset table by table: a collated table live, another served", async () => {
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
      expect(
         ci.sent.some((q) =>
            q.includes("`proj`.`ds`.INFORMATION_SCHEMA.TABLES"),
         ),
      ).toBe(true);
      const plain = await refusal(
         catalog,
         "run: plain -> { group_by: region; order_by: region }",
      );
      expect(plain.why).toBeUndefined();
   });

   it("past the dataset bound, does not read a dataset the project does not list", async () => {
      const { why } = await refusal(
         {
            location: "US",
            projects: { proj: ["ds2"] },
            datasets: { "proj.ds": CI_DS },
         },
         "run: ci -> { group_by: region }",
         STRING_READS,
         0,
      );
      expect(why).toBeUndefined();
   });

   it("serves a .table() source when the project read fails, from its dataset's own read", async () => {
      const { why } = await refusal(
         { location: "US", projects: { proj: "fail" } },
         "run: plain -> { group_by: region }",
      );
      expect(why).toBeUndefined();
   });

   it("past the dataset bound, answers live when the project read fails", async () => {
      const { why } = await refusal(
         { location: "US", projects: { proj: "fail" } },
         "run: plain -> { group_by: region }",
         STRING_READS,
         0,
      );
      expect(why).toBe(LIVE);
   });

   it("answers live when a collated dataset's read fails", async () => {
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
      const { why } = await refusal(
         { location: "US" },
         "run: plain -> { where: region = 'a COLLATE b'; group_by: region }",
      );
      expect(why).toBe(LIVE);
   });
});

describe("a BigQuery source's coverage, by table", () => {
   it("answers live when the dataset's own read does not see the table", async () => {
      const { why } = await refusal(
         { location: "US", unseenInDataset: ["ds.plain_t"] },
         "run: plain -> { group_by: region }",
      );
      expect(why).toBe(LIVE);
   });

   it("past the dataset bound, answers live when the project-wide read does not see the table", async () => {
      const { why } = await refusal(
         { location: "US", unseen: ["ds.plain_t"] },
         "run: plain -> { group_by: region }",
         STRING_READS,
         0,
      );
      expect(why).toBe(LIVE);
   });

   it("answers live when a collated dataset's read does not see the table", async () => {
      // A hidden collated table beside visible ones: its dataset is listed
      // as collated, but the table itself must be seen.
      const { why } = await refusal(
         {
            location: "US",
            projects: { proj: ["ds"] },
            datasets: { "proj.ds": CI_DS },
            unseenInDataset: ["ds.ci_t"],
         },
         "run: ci -> { group_by: region }",
      );
      expect(why).toBe(LIVE);
   });

   it("serves a view only when its project holds no collated column", async () => {
      const view = { "ds.plain_t": "VIEW" };
      const clean = await refusal(
         { location: "US", types: view },
         "run: plain -> { group_by: region }",
      );
      expect(clean.why).toBeUndefined();
      // A collated column anywhere in the project: the view's columns are
      // not documented to carry what it reads, so it answers live.
      const dirty = await refusal(
         { location: "US", types: view, projects: { proj: ["ds2"] } },
         "run: plain -> { group_by: region }",
      );
      expect(dirty.why).toBe(LIVE);
   });
});

describe("a BigQuery source's string comparison, with no location set", () => {
   it("reads each region datasets.list names, an .sql() source served", async () => {
      const { why, regions } = await refusal(
         { listed: "EU" },
         "run: authored -> { group_by: region }",
      );
      expect(why).toBeUndefined();
      expect(regions).toContain("eu");
   });

   it("answers an .sql() source live when a derived region lists a collated dataset", async () => {
      const { why } = await refusal(
         { listed: "EU", projects: { proj: ["ds"] } },
         "run: authored -> { group_by: region }",
      );
      expect(why).toBe(LIVE);
   });

   it("answers an .sql() source live when datasets.list names no dataset", async () => {
      const { why } = await refusal(
         { listed: "none" },
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

   it("answers a view live: no project to read", async () => {
      const { why } = await refusal(
         { types: { "ds.plain_t": "VIEW" } },
         "run: plain -> { group_by: region }",
      );
      expect(why).toBe(LIVE);
   });

   it("answers an .sql() source or a raw SQL expression live: no project to read", async () => {
      for (const q of [
         "run: authored -> { group_by: region }",
         "run: plain_sql -> { group_by: r }",
      ]) {
         const { why } = await refusal({}, q);
         expect(why).toBe(LIVE);
      }
   });
});

describe("a BigQuery result's re-grouping and ordering", () => {
   it("is judged by the same lookup, as the result rule reads it", async () => {
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
         (
            await refusal(
               {
                  location: "US",
                  projects: { proj: ["ds"] },
                  datasets: { "proj.ds": CI_DS },
               },
               q,
               reads?.grouped,
            )
         ).why,
      ).toBe(LIVE);
   });
});

/**
 * A BigQuery database table's stored side, looked up: the source's side from
 * what a bind recorded of the catalog, the table's from the collation its
 * build recorded in its contract. The lookup twin of the probe path's five
 * cases, with the same outcomes: a table is kept only when every collated
 * column it records is the one its source compares under, and declined
 * for any other collation, on either side.
 */
describe("a BigQuery database table, looked up", () => {
   const stored = async (
      catalog: Catalog,
      query: string,
      collation: string | null,
   ) => {
      const { connection } = stubBigQuery(catalog);
      const runtime = new SingleConnectionRuntime({ connection });
      const materializer = runtime.loadModel(MODEL);
      recordStringCapabilities(
         runtime,
         await readStringCapabilities({
            modelDef: (await materializer.getModel())._modelDef,
            load: (q) => materializer.loadQuery(q),
            connection: async () => connection,
         }),
      );
      const contract = parseContract({
         v: 1,
         dialect: "standardsql",
         columns: [
            { name: "id", nativeType: "INT64", collation: null },
            { name: "region", nativeType: "STRING", collation },
         ],
      })!;
      return lookupStoredRefusal({
         tables: [
            {
               tier: "database",
               tablePath: "proj.ds.copy_t",
               virtualHandle: "h",
            },
         ],
         authorRunnable: materializer.loadQuery(query),
         reads: STRING_READS,
         bind: makeSnapshot(
            [],
            {},
            undefined,
            undefined,
            new Map([["h", contract]]),
         ),
      });
   };
   const UNMATCHED =
      "a stored table compares strings under a unknown collation (a BigQuery table unmatched)";

   it("is kept when neither it nor its source holds a collated string", async () => {
      expect(
         await stored(
            { location: "US" },
            "run: plain -> { group_by: region }",
            null,
         ),
      ).toBeUndefined();
   });

   it("answers live when its source lost the collation its copy kept", async () => {
      expect(
         await stored(
            {
               location: "US",
               projects: { proj: ["ds"] },
               datasets: { "proj.ds": { copy_t: { region: "und:ci" } } },
            },
            "run: plain -> { group_by: region }",
            "bigquery:und:ci",
         ),
      ).toBe(UNMATCHED);
   });

   it("answers live when its source's column is collated and its own is not", async () => {
      expect(
         await stored(
            {
               location: "US",
               projects: { proj: ["ds"] },
               datasets: { "proj.ds": CI_DS },
            },
            "run: ci -> { group_by: region }",
            null,
         ),
      ).toBe(UNMATCHED);
   });

   it("is kept when its column recorded the source's collation", async () => {
      expect(
         await stored(
            {
               location: "US",
               projects: { proj: ["ds"] },
               datasets: {
                  "proj.ds": { ...CI_DS, copy_t: { region: "und:ci" } },
               },
            },
            "run: ci -> { group_by: region }",
            "bigquery:und:ci",
         ),
      ).toBeUndefined();
   });

   it("is kept, at a string grain, when its contract recorded bigquery:NULL (0.18.0 to 0.18.2) for an uncollated column", async () => {
      // BigQuery's catalog reports an uncollated column's collation_name as
      // the string 'NULL', which those builds recorded as a collation.
      expect(
         await stored(
            { location: "US" },
            "run: plain -> { group_by: region }",
            "bigquery:NULL",
         ),
      ).toBeUndefined();
   });

   it("answers live when its column recorded another collation", async () => {
      expect(
         await stored(
            {
               location: "US",
               projects: { proj: ["ds"] },
               datasets: {
                  "proj.ds": { ...CI_DS, copy_t: { region: "und:cs" } },
               },
            },
            "run: ci -> { group_by: region }",
            "bigquery:und:cs",
         ),
      ).toBe(UNMATCHED);
   });
});

/**
 * The build side's read (a `storage=` build's column contract): the same
 * catalog reads over the build's SQL and the tables its source reads, so a
 * build records its strings compared `exact` exactly where an answer's
 * check would compare them byte-exact, and `none` (fail closed) otherwise.
 */
describe("a BigQuery storage= build's live string comparison, for its contract", () => {
   async function buildBinary(catalog: Catalog, name: string) {
      const { connection, sent } = stubBigQuery(catalog);
      const model = new SingleConnectionRuntime({ connection }).loadModel(
         MODEL,
      );
      const query = model.loadQuery(`run: ${name} -> { select: * }`);
      const { sql } = await query.getPreparedResult();
      const def = (
         (await model.getModel()) as unknown as {
            _modelDef: { contents: Record<string, unknown> };
         }
      )._modelDef.contents[name];
      const binary = await bigqueryBuildCompareBinary({
         connection,
         source: { connectionName: "bq", _sourceDef: def } as never,
         sql,
      });
      return { binary, sent };
   }

   it("is binary where an answer's check is: no collated column in the project", async () => {
      expect((await buildBinary({ location: "US" }, "plain")).binary).toBe(
         true,
      );
      expect((await buildBinary({ location: "US" }, "authored")).binary).toBe(
         true,
      );
   });

   it("is binary over a table outside the collated dataset's collated tables", async () => {
      const catalog = {
         location: "US",
         projects: { proj: ["ds"] },
         datasets: { "proj.ds": CI_DS },
      };
      expect((await buildBinary(catalog, "plain")).binary).toBe(true);
      expect((await buildBinary(catalog, "ci")).binary).toBe(false);
   });

   it("is not binary when a read fails or cannot place the project", async () => {
      expect(
         (
            await buildBinary(
               { location: "US", datasets: { "proj.ds": "fail" } },
               "plain",
            )
         ).binary,
      ).toBe(false);
      expect(
         (
            await buildBinary(
               { location: "US", projects: { proj: ["ds"] } },
               "authored",
            )
         ).binary,
      ).toBe(false);
   });

   it("reads only the datasets a .table() build reads, never the project-wide region views, so a dataset's READER proves it", async () => {
      // The project-wide views are denied, as to a dataset-scoped account.
      const denied = { location: "US", projects: { proj: "fail" as const } };
      const plain = await buildBinary(denied, "plain");
      expect(plain.binary).toBe(true);
      expect(plain.sent).toHaveLength(1);
      expect(plain.sent[0]).toContain("`proj`.`ds`.INFORMATION_SCHEMA.TABLES");
      expect(plain.sent.join("\n")).not.toContain("region-");
      // A dataset of the job's own project, read in that project.
      const other = await buildBinary(denied, "other");
      expect(other.binary).toBe(true);
      expect(other.sent).toHaveLength(1);
      expect(other.sent[0]).toContain("FROM `ds2`.INFORMATION_SCHEMA.TABLES");
   });

   it("reads an .sql() build's projects region-wide: its tables are not known up front", async () => {
      const authored = await buildBinary({ location: "US" }, "authored");
      expect(authored.binary).toBe(true);
      expect(authored.sent.join("\n")).toContain(
         "`proj`.`region-us`.INFORMATION_SCHEMA",
      );
      expect(
         (
            await buildBinary(
               { location: "US", projects: { proj: "fail" } },
               "authored",
            )
         ).binary,
      ).toBe(false);
   });
});

/**
 * Each reason a build's catalog read answers not binary, told once with the
 * project, location, dataset and table it read, and never a driver's text
 * (the stub's failures say "Access Denied", which nothing told carries).
 */
describe("a BigQuery storage= build's unproved read, told why", () => {
   async function unread(
      catalog: Catalog,
      name: string,
      options: {
         sql?: (sql: string) => string;
         connectionName?: string;
         noProject?: true;
         datasetReadLimit?: number;
      } = {},
   ): Promise<{ binary: boolean; told: Record<string, unknown>[] }> {
      const { connection } = stubBigQuery(catalog);
      const model = new SingleConnectionRuntime({ connection }).loadModel(
         MODEL,
      );
      const { sql } = await model
         .loadQuery(`run: ${name} -> { select: * }`)
         .getPreparedResult();
      const def = (
         (await model.getModel()) as unknown as {
            _modelDef: { contents: Record<string, unknown> };
         }
      )._modelDef.contents[name];
      if (options.noProject) {
         Object.assign(connection as object, {
            projectId: "",
            billingProjectId: "",
         });
      }
      const told: Record<string, unknown>[] = [];
      const binary = await bigqueryBuildCompareBinary({
         connection,
         source: {
            name,
            connectionName: options.connectionName ?? "bq",
            _sourceDef: def,
         } as never,
         sql: options.sql ? options.sql(sql) : sql,
         onUnread: (unread) => told.push({ ...unread }),
         ...(options.datasetReadLimit !== undefined
            ? { datasetReadLimit: options.datasetReadLimit }
            : {}),
      });
      for (const t of told) {
         expect(JSON.stringify(t)).not.toContain("Access Denied");
      }
      return { binary, told };
   }

   const CASES: {
      reason: string;
      catalog: Catalog;
      name: string;
      options?: Parameters<typeof unread>[2];
      expected: Record<string, unknown>;
   }[] = [
      {
         reason: "collate_in_sql",
         catalog: { location: "US" },
         name: "plain",
         options: { sql: (sql) => `${sql} -- COLLATE` },
         expected: { project: "proj" },
      },
      {
         reason: "no_job_project",
         catalog: { location: "US" },
         name: "plain",
         options: { noProject: true },
         expected: {},
      },
      {
         reason: "table_on_other_connection",
         catalog: { location: "US" },
         name: "plain",
         options: { connectionName: "other_bq" },
         expected: { project: "proj" },
      },
      {
         reason: "table_unplaceable",
         catalog: { location: "US" },
         name: "wild",
         expected: { project: "proj" },
      },
      {
         reason: "table_types_unread",
         catalog: { location: "US", projects: { proj: "fail" } },
         name: "plain",
         options: { datasetReadLimit: 0 },
         expected: { project: "proj" },
      },
      {
         reason: "table_not_listed",
         catalog: { location: "US", unseen: ["ds.plain_t"] },
         name: "plain",
         options: { datasetReadLimit: 0 },
         expected: { project: "proj", dataset: "ds", table: "plain_t" },
      },
      {
         reason: "dataset_unread",
         catalog: { location: "US", datasets: { "proj.ds": "fail" } },
         name: "plain",
         expected: { project: "proj", dataset: "ds", table: "plain_t" },
      },
      {
         reason: "project_unread",
         catalog: { location: "US", projects: { proj: "fail" } },
         name: "authored",
         expected: { project: "proj" },
      },
      {
         reason: "project_collated",
         catalog: { location: "US", projects: { proj: ["ds"] } },
         name: "authored",
         expected: { project: "proj" },
      },
      {
         reason: "dataset_table_not_listed",
         catalog: {
            location: "US",
            projects: { proj: ["ds"] },
            datasets: { "proj.ds": CI_DS },
            unseenInDataset: ["ds.ci_t"],
         },
         name: "ci",
         expected: { project: "proj", dataset: "ds", table: "ci_t" },
      },
      {
         reason: "read_failed",
         catalog: {
            location: "US",
            projects: { proj: ["ds"] },
            datasets: { "proj.ds": "fail" },
         },
         name: "ci",
         options: { datasetReadLimit: 0 },
         expected: { project: "proj", dataset: "ds", table: "ci_t" },
      },
      {
         reason: "project_unread",
         catalog: {
            location: "US",
            projects: { proj: "fail" },
            types: { "ds.plain_t": "VIEW" },
         },
         name: "plain",
         expected: {
            project: "proj",
            dataset: "ds",
            table: "plain_t",
            tableType: "VIEW",
         },
      },
   ];

   for (const c of CASES) {
      it(`answers not binary and tells ${c.reason}`, async () => {
         const { binary, told } = await unread(c.catalog, c.name, c.options);
         expect(binary).toBe(false);
         expect(told).toEqual([
            { reason: c.reason, location: "us", ...c.expected },
         ]);
      });
   }

   it("tells nothing when the read proves the strings, or proves them collated", async () => {
      expect(await unread({ location: "US" }, "plain")).toEqual({
         binary: true,
         told: [],
      });
      const collated = await unread(
         {
            location: "US",
            projects: { proj: ["ds"] },
            datasets: { "proj.ds": CI_DS },
         },
         "ci",
      );
      expect(collated).toEqual({ binary: false, told: [] });
   });
});

/**
 * A BigQuery database build's contract capture, from the catalog as BigQuery
 * answers it: an uncollated column's `collation_name` is the string `'NULL'`,
 * which compares bytes, and is recorded `binary`, as the serve side's catalog
 * read takes it.
 */
describe("a BigQuery build's contract capture", () => {
   it("records binary for a column the catalog reports collated 'NULL', and a real collation as itself", async () => {
      const sent: string[] = [];
      const connection = {
         dialectName: "standardsql",
         runSQL: async (sql: string) => {
            sent.push(sql);
            const rows = sql.includes("collation_name")
               ? [
                    {
                       table_name: "t",
                       column_name: "id",
                       collation_name: "NULL",
                    },
                    {
                       table_name: "t",
                       column_name: "region",
                       collation_name: "NULL",
                    },
                    {
                       table_name: "t",
                       column_name: "ci",
                       collation_name: "und:ci",
                    },
                 ]
               : [
                    { table_name: "t", column_name: "id", data_type: "INT64" },
                    {
                       table_name: "t",
                       column_name: "region",
                       data_type: "STRING",
                    },
                    { table_name: "t", column_name: "ci", data_type: "STRING" },
                 ];
            return { rows, totalRows: rows.length };
         },
      };
      const contract = await captureDatabaseContract({
         connection: connection as never,
         tableName: "ds.t",
         physicalTableName: "ds.t",
      });
      expect(sent.some((sql) => sql.includes("collation_name"))).toBe(true);
      expect(
         Object.fromEntries(contract.columns.map((c) => [c.name, c.collation])),
      ).toEqual({ id: null, region: "binary", ci: "bigquery:und:ci" });
   });
});

/**
 * Credible's e2e S27-02 (0.18.6): a `.table()` source in one dataset, its
 * account granted only that dataset (READER, jobUser: no project-level
 * `bigquery.tables.list`, so every region-qualified `INFORMATION_SCHEMA`
 * read is denied), and a `storage=` chain over it. Both stored tables built;
 * a query over the chain was declined at read admission, the source side
 * unread, and answered live. Judged here as the served copy's answer is: the
 * real admission rule over the lookups, over what a bind records.
 */
describe("a dataset-scoped BigQuery account's storage= chain, served", () => {
   const MODEL_S27 = `##! experimental { persistence }
source: users is bq.table('ecomm_e2e.users')
#@ persist storage="credible"
source: users_s is users -> { select: id, state, gender, age }
#@ persist storage="credible"
source: by_state is users_s -> { group_by: state, gender; aggregate: n is count() } extend { measure: total is n.sum() }
`;
   const FIELDS = [
      { name: "id", type: "number" },
      { name: "state", type: "string" },
      { name: "gender", type: "string" },
      { name: "age", type: "number" },
   ];
   /** Every region-qualified read denied, as to a dataset's READER. */
   const DATASET_SCOPED: Catalog = {
      location: "US",
      projects: { proj: "fail" },
   };
   const QUERY = "run: by_state -> { group_by: state; aggregate: total }";
   /** The served copy's answer: one `storage=` table read. */
   const STORED: TableRead[] = [{ tier: "storage" }];

   async function admitted(catalog: Catalog, query = QUERY) {
      const { connection, sent } = stubBigQuery({ fields: FIELDS, ...catalog });
      const runtime = new SingleConnectionRuntime({ connection });
      const materializer = runtime.loadModel(MODEL_S27);
      const told: Record<string, unknown>[] = [];
      recordStringCapabilities(
         runtime,
         await readStringCapabilities({
            modelDef: (await materializer.getModel())._modelDef,
            load: (q) => materializer.loadQuery(q),
            connection: async () => connection,
            onUnread: (unread) => told.push({ ...unread }),
         }),
      );
      const authorRunnable = materializer.loadQuery(query);
      const reads = await resultReadsOfQuery(authorRunnable, new Set(), {
         fromStage: 0,
      });
      let facts: ReadFacts = {
         pastEntryPointGate: false,
         compositeRowFilter: false,
         reads: { scoped: false, gated: false },
         columnsListed: true,
         resultOrder: { applies: true, reads: "read", tables: STORED },
         probes: {},
      };
      // As read admission asks: each probe the rule needs, then again.
      for (;;) {
         const verdict = admitRead("served", STORED, facts, {
            materialize: true,
            gatedSources: true,
         } as never);
         if (verdict.admit !== "needs_probe") {
            for (const t of told) {
               expect(JSON.stringify(t)).not.toContain("Access Denied");
            }
            return { verdict, sent, told };
         }
         const { comparison, side } = verdict.probe;
         const compared =
            comparison === "result_ordered" ? reads?.ordered : reads?.grouped;
         const refusal =
            side === "source"
               ? await lookupSourceRefusal({ authorRunnable, reads: compared })
               : await lookupStoredRefusal({
                    tables: [],
                    authorRunnable,
                    reads: compared,
                 });
         facts = {
            ...facts,
            probes: {
               ...facts.probes,
               [comparison]: {
                  ...facts.probes?.[comparison],
                  [side]: { refusal },
               },
            },
         };
      }
   }

   it("serves the chain from storage, reading only the dataset's own catalog", async () => {
      const { verdict, sent, told } = await admitted(DATASET_SCOPED);
      expect(verdict).toEqual({ admit: true, pastEntryPointGate: false });
      expect(sent).toHaveLength(1);
      expect(sent[0]).toContain("FROM `ecomm_e2e`.INFORMATION_SCHEMA.TABLES");
      expect(sent.join("\n")).not.toContain("region-");
      expect(told).toEqual([]);
   });

   it("still answers live when the source's column is collated", async () => {
      const { verdict, told } = await admitted({
         ...DATASET_SCOPED,
         datasets: { ecomm_e2e: { users: { state: "und:ci" } } },
      });
      expect(verdict).toEqual({
         admit: false,
         reason: "result_order_comparison",
         detail: `the result groups or compares a string a stored table does not compare as its source does: ${LIVE}`,
      });
      // A collation read is no failure: nothing to tell.
      expect(told).toEqual([]);
   });

   it("answers live when the dataset's own read is denied too, and tells why and where", async () => {
      const { verdict, told } = await admitted({
         ...DATASET_SCOPED,
         datasets: { ecomm_e2e: "fail" },
      });
      expect(verdict).toEqual({
         admit: false,
         reason: "result_order_comparison",
         detail: `the result groups or compares a string a stored table does not compare as its source does: ${LIVE}`,
      });
      expect(told).toEqual([
         {
            connectionName: "bq",
            reason: "dataset_unread",
            project: "proj",
            location: "us",
            dataset: "ecomm_e2e",
         },
      ]);
   });

   it("reads project-wide past the dataset bound, and tells the denied read", async () => {
      const { connection } = stubBigQuery({
         fields: FIELDS,
         ...DATASET_SCOPED,
      });
      const runtime = new SingleConnectionRuntime({ connection });
      const materializer = runtime.loadModel(MODEL_S27);
      const past: Record<string, unknown>[] = [];
      const capabilities = await readStringCapabilities({
         modelDef: (await materializer.getModel())._modelDef,
         load: (q) => materializer.loadQuery(q),
         connection: async () => connection,
         onUnread: (unread) => past.push({ ...unread }),
         datasetReadLimit: 0,
      });
      recordStringCapabilities(runtime, capabilities);
      expect(
         await lookupSourceRefusal({
            authorRunnable: materializer.loadQuery(
               "run: users -> { group_by: state }",
            ),
            reads: STRING_READS_ON("state"),
         }),
      ).toBe(LIVE);
      expect(past.map((t) => t.reason)).toContain("project_unread");
   });
});

/** {@link STRING_READS} over one other string column. */
function STRING_READS_ON(column: string): GateReads {
   return {
      ...STRING_READS,
      names: new Set([column]),
      columns: [[column]],
      entryPaths: [[column]],
   };
}
