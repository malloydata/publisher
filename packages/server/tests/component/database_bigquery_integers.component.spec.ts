// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database `#@ persist` table on BigQuery and on DuckDB whose manifest entry
 * carries no recorded warehouse types (no `nativeSchema` and no `schema`,
 * which manifests written before the control plane stored them lack). Such an
 * entry records nothing to compare the table's columns with, so a change since
 * the build cannot be ruled out: the bind leaves the table out and the source
 * answers live, with the serve verdict, the path report and the package
 * warning all naming `types_unrecorded`, on BigQuery as on any dialect (it
 * was served, reading its big integers through INT64, through 0.17.1). A table
 * whose build captured the types serves through its native casts, and gets no
 * warning.
 *
 * BigQuery is a stand-in: its schema reads come from a fixture and its SQL
 * returns no rows, since what is checked is the served copy and the SQL it
 * writes, not the warehouse.
 */

import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { BigQueryConnection } from "@malloydata/db-bigquery";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import { unrecordedAudit } from "../../src/service/access_record";
import { QueryPrivileges } from "../../src/service/model_query_request";
import { Package } from "../../src/service/package";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

/**
 * BigQuery with fixture schemas: `k` STRING and `n` INT64 for any SQL. Its
 * schema read is private, so it is replaced on the instance.
 */
function standInBigQuery(name: string): Connection {
   const connection = new BigQueryConnection({
      name,
      projectId: "stand-in",
   } as never);
   // The collation probe's datasets.list, which names the region its
   // project-wide read runs in.
   Object.assign((connection as unknown as { bigQuery: object }).bigQuery, {
      getDatasets: async () => [[{ metadata: { location: "US" } }]],
   });
   Object.assign(connection, {
      getSQLBlockSchema: async () => ({
         fields: [
            { name: "k", type: "STRING", mode: "NULLABLE" },
            { name: "n", type: "INT64", mode: "NULLABLE" },
         ],
      }),
      // The bind's catalog read of the stored table's columns, as BigQuery
      // types them, and the collation probe's read of the stored table's
      // type; every other statement returns nothing (the collation probe's
      // other reads among them: no collated column).
      runSQL: async (sql: string) =>
         /INFORMATION_SCHEMA\.TABLES\b/.test(sql)
            ? {
                 rows: [
                    {
                       table_schema: "scratch",
                       table_name: "by_k_table",
                       table_type: "BASE TABLE",
                    },
                 ],
                 totalRows: 1,
              }
            : sql.includes("INFORMATION_SCHEMA.COLUMNS") &&
                !sql.includes("collation_name")
              ? {
                   rows: [
                      {
                         table_name: "by_k_table",
                         column_name: "k",
                         data_type: "STRING",
                      },
                      {
                         table_name: "by_k_table",
                         column_name: "total",
                         data_type: "INT64",
                      },
                   ],
                   totalRows: 2,
                }
              : { rows: [], totalRows: 0 },
   });
   return connection;
}

const modelOn = (
   connection: string,
   sql: string,
) => `##! experimental { persistence }

#@ persist name="scratch.by_k"
source: by_k is ${connection}.sql("${sql}") -> {
  group_by: k
  aggregate: total is n.sum()
} extend {
  measure: grand_total is total.sum()
}
`;

let work: string | undefined;
let restore: (() => void) | undefined;
afterEach(() => {
   restore?.();
   if (work) fs.rmSync(work, { recursive: true, force: true });
   work = undefined;
});

/**
 * Load the package over `connections`, bind its one table database with no
 * captured types, and return the served copy's problems and the path report
 * of a query reading the big-integer column.
 */
async function bindWithoutNativeSchema(
   model: string,
   connections: Map<string, Connection>,
   nativeSchema?: { name: string; type: string }[],
) {
   restore = installSettings({
      env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_PACKAGE_VERSIONING: "off" },
   }).restore;
   work = fs.mkdtempSync(path.join(os.tmpdir(), "database-bq-int-"));
   const dir = path.join(work, "pkg");
   writePackageFiles(
      dir,
      { "m.malloy": model },
      { manifest: { name: "pkg", description: "fixture" } },
   );
   connections.set("duckdb", new DuckDBConnection("duckdb", ":memory:"));
   const pkg = await Package.create("env", "pkg", dir, connections);
   const sources = Object.values(pkg.getBuildPlan()?.sources ?? {});
   expect(sources.map((s) => s.name)).toEqual(["by_k"]);
   const [source] = sources;
   // The stored table, for the bind to read its columns: on DuckDB it is
   // created on the source's connection; the BigQuery stand-in answers the
   // catalog read itself.
   if (source.connectionName === "duckdb") {
      const duckdb = await pkg.getMalloyConnection("duckdb");
      await duckdb.runSQL("CREATE SCHEMA IF NOT EXISTS scratch");
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE scratch.by_k_table AS SELECT 'a' AS k, CAST(1 AS BIGINT) AS total",
      );
   }
   await pkg.reloadAllModels({
      [source.sourceEntityId]: {
         tableName: "scratch.by_k_table",
         connectionName: source.connectionName,
         freshnessWindowSeconds: 86400,
         freshnessFallback: "live",
         dataAsOf: new Date().toISOString(),
         ...(nativeSchema ? { nativeSchema } : {}),
      },
   } as never);
   const model_ = pkg.getModel("m.malloy");
   await model_?.prepareServedCopy();
   const result = await model_!.runQuery(
      unrecordedAudit(),
      { query: "run: by_k -> { group_by: k; aggregate: grand_total }" },
      QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
   );
   return {
      notices: (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === "database-native-type-missing",
      ),
      codes: (pkg.getPackageMetadata().warnings ?? []).map((w) => w.code),
      verdict: pkg
         .materialization()
         .serveVerdicts()
         .find((v) => v.sourceName === "by_k"),
      declines: (result.path?.considered ?? []).map(
         (c) => (c as { code?: string }).code,
      ),
      problems: model_?.servedCopyProblems() ?? [],
      servedBy: result.path?.servedBy,
      sql: String((result.result as unknown as { sql?: string }).sql ?? ""),
   };
}

describe("a database table bound with no captured warehouse types", () => {
   // A typeless entry answers live (888c55948): it was served before.
   for (const [dialect, model, connections] of [
      [
         "BigQuery",
         modelOn("bq", "SELECT k, n FROM t"),
         () => new Map<string, Connection>([["bq", standInBigQuery("bq")]]),
      ],
      [
         "DuckDB",
         modelOn("duckdb", "SELECT 'a' AS k, CAST(1 AS BIGINT) AS n"),
         () => new Map<string, Connection>(),
      ],
   ] as const) {
      it(`answers live on ${dialect}, naming types_unrecorded everywhere`, async () => {
         const { codes, verdict, declines, servedBy, notices } =
            await bindWithoutNativeSchema(model, connections());
         expect(servedBy?.kind).toBe("live");
         expect(verdict).toMatchObject({
            verdict: "live",
            reason: "types_unrecorded",
         });
         expect(declines).toContain("types_unrecorded");
         expect(codes).toContain("materialization-types-unrecorded");
         // Left out whole: no column of it is read, so none is warned of.
         expect(notices).toEqual([]);
      });
   }

   it("warns of nothing for a table whose build captured the types", async () => {
      const { notices, problems, sql } = await bindWithoutNativeSchema(
         modelOn("bq", "SELECT k, n FROM t"),
         new Map<string, Connection>([["bq", standInBigQuery("bq")]]),
         [
            { name: "k", type: "STRING" },
            { name: "total", type: "INT64" },
         ],
      );
      expect(problems).toEqual([]);
      expect(sql).toMatch(/AS INT64\)/);
      expect(notices).toEqual([]);
   });
});
