// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A BigQuery source's catalog reads, against a catalog that answers as
 * BigQuery does: its `INFORMATION_SCHEMA` SQL is run for real (in DuckDB,
 * over tables standing in for the views), its rows come back in no
 * particular order, one page of them (10 unless the read asks for more),
 * with `totalRows` reporting the whole answer, and an uncollated column's
 * `collation_name` is the string `'NULL'`.
 *
 * The project holds 47 datasets of uncollated tables and one, `zz`, whose
 * table holds an `und:ci` column. Each case runs under 20 row orders: a read
 * that saw only some rows, or took `'NULL'` for a collation, answers
 * differently from one order to the next.
 */

import { BigQueryConnection } from "@malloydata/db-bigquery";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { type Connection, SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { lookupSourceRefusal } from "./admission_lookup";
import {
   readStringCapabilities,
   recordStringCapabilities,
} from "./source_capability";
import type { GateReads } from "./gate_reads_types";

const duckdb = new DuckDBConnection("bq-catalog-spec", ":memory:");

/** The uncollated datasets: d00 … d46, each holding `t` (and d01 `copy_t`). */
const PLAIN = Array.from(
   { length: 47 },
   (_, i) => `d${String(i).padStart(2, "0")}`,
);

beforeAll(async () => {
   await duckdb.runSQL(
      `CREATE TABLE tabs (project VARCHAR, region VARCHAR, table_schema VARCHAR, table_name VARCHAR, table_type VARCHAR)`,
   );
   await duckdb.runSQL(
      `CREATE TABLE cols (project VARCHAR, region VARCHAR, table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR, collation_name VARCHAR)`,
   );
   await duckdb.runSQL(
      `CREATE TABLE fps (project VARCHAR, region VARCHAR, table_schema VARCHAR, table_name VARCHAR, field_path VARCHAR, collation_name VARCHAR)`,
   );
   const tables: [string, string, Record<string, string>][] = [
      ...PLAIN.map(
         (d) =>
            [d, "t", { id: "NULL", region: "NULL" }] as [
               string,
               string,
               Record<string, string>,
            ],
      ),
      ["d01", "copy_t", { id: "NULL", region: "NULL" }],
      // Collated datasets c00 … c09 (for the cut page), then zz.
      ...Array.from(
         { length: 10 },
         (_, i) =>
            [`c${String(i).padStart(2, "0")}`, "t", { region: "und:ci" }] as [
               string,
               string,
               Record<string, string>,
            ],
      ),
      ["zz", "ci_t", { id: "NULL", region: "und:ci" }],
   ];
   for (const [d, t, columns] of tables) {
      await duckdb.runSQL(
         `INSERT INTO tabs VALUES ('proj', 'us', '${d}', '${t}', 'BASE TABLE')`,
      );
      for (const [c, collation] of Object.entries(columns)) {
         for (const view of ["cols", "fps"]) {
            await duckdb.runSQL(
               `INSERT INTO ${view} VALUES ('proj', 'us', '${d}', '${t}', '${c}', '${collation}')`,
            );
         }
      }
   }
});

afterAll(async () => {
   await duckdb.close();
});

const VIEWS: Record<string, string> = {
   TABLES: "tabs",
   COLUMNS: "cols",
   COLUMN_FIELD_PATHS: "fps",
};

/** A seeded shuffle: BigQuery returns rows of a read with no ORDER BY in no set order. */
function shuffled<T>(items: T[], seed: number): T[] {
   let s = seed + 1;
   const next = () => {
      s = (s * 1103515245 + 12345) % 2147483648;
      return s / 2147483648;
   };
   const out = [...items];
   for (let i = out.length - 1; i > 0; i--) {
      const j = Math.floor(next() * (i + 1));
      [out[i], out[j]] = [out[j], out[i]];
   }
   return out;
}

/**
 * A BigQuery connection (project `proj`, location US) whose catalog reads
 * run against the tables above. `pageCap`, when set, ends every page at that
 * many rows whatever the read asks for, as BigQuery's 10 MB page does.
 */
function stubBigQuery(seed: number, pageCap?: number): Connection {
   const connection = new BigQueryConnection({
      name: "bq",
      projectId: "proj",
      billingProjectId: "proj",
      location: "US",
   });
   const stub = connection as unknown as Record<string, unknown>;
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
   stub.runSQL = async (sql: string, options?: { rowLimit?: number }) => {
      const local = sql.replace(
         /`([^`]+)`(?:\.`([^`]+)`)?\.INFORMATION_SCHEMA\.(COLUMN_FIELD_PATHS|COLUMNS|TABLES)/g,
         (_m, a: string, b: string | undefined, view: string) => {
            const [project, scope] = b === undefined ? ["proj", a] : [a, b];
            const where = scope.startsWith("region-")
               ? `region = '${scope.slice("region-".length)}'`
               : `table_schema = '${scope}'`;
            return `(SELECT * FROM ${VIEWS[view]} WHERE project = '${project}' AND ${where})`;
         },
      );
      const all = shuffled(
         (await duckdb.runSQL(local, { rowLimit: 1_000_000 })).rows,
         seed,
      );
      const limit = Math.min(options?.rowLimit ?? 10, pageCap ?? Infinity);
      return { rows: all.slice(0, limit), totalRows: all.length };
   };
   return connection as unknown as Connection;
}

const MODEL = `
source: plain is bq.table('proj.d00.t')
source: ci is bq.table('proj.zz.ci_t')
`;

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

const RUNS = 20;

/** The gate's answer for `query`, once per row order, each on a fresh runtime. */
async function answers(
   query: string,
   pageCap?: number,
): Promise<(string | undefined)[]> {
   const out: (string | undefined)[] = [];
   for (let seed = 0; seed < RUNS; seed++) {
      // Read as a bind reads the catalog, then looked up.
      const connection = stubBigQuery(seed, pageCap);
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
      out.push(
         await lookupSourceRefusal({
            authorRunnable: materializer.loadQuery(query),
            reads: STRING_READS,
         }),
      );
   }
   return out;
}

describe("a BigQuery catalog read, as BigQuery pages and reports it", () => {
   it("answers live for a collated table whose dataset is past the first page of ten", async () => {
      expect(await answers("run: ci -> { group_by: region }")).toEqual(
         Array(RUNS).fill(LIVE),
      );
   });

   it("serves a table whose columns BigQuery reports with the collation 'NULL'", async () => {
      expect(await answers("run: plain -> { group_by: region }")).toEqual(
         Array(RUNS).fill(undefined),
      );
   });

   it("answers live when a page ends before every row the read reports", async () => {
      // Eleven collated datasets, five rows a page: the read is cut.
      expect(await answers("run: ci -> { group_by: region }", 5)).toEqual(
         Array(RUNS).fill(LIVE),
      );
      expect(await answers("run: plain -> { group_by: region }", 5)).toEqual(
         Array(RUNS).fill(LIVE),
      );
   });
});
