// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A colocated `#@ persist` table on BigQuery whose manifest entry carries no
 * captured warehouse types (`nativeSchema`, which manifests written before the
 * control plane stored it lack). A column whose field Malloy types as a
 * `bigint` number is read through a cast to its warehouse type; BigQuery has
 * exactly one integer type, INT64, so the served copy reads it through INT64
 * and the source serves. On any other dialect nothing captured means no cast
 * is known exact, and the column is still left out.
 *
 * Either way the package warns once for the source
 * (`stored-native-schema-missing`), naming its columns; a table whose build
 * captured the types gets no such warning.
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
   Object.assign(connection, {
      getSQLBlockSchema: async () => ({
         fields: [
            { name: "k", type: "STRING", mode: "NULLABLE" },
            { name: "n", type: "INT64", mode: "NULLABLE" },
         ],
      }),
      // The bind's catalog read of the stored table's columns, as BigQuery
      // types them; every other statement returns nothing.
      runSQL: async (sql: string) =>
         sql.includes("INFORMATION_SCHEMA.COLUMNS")
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
 * Load the package over `connections`, bind its one table colocated with no
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
   work = fs.mkdtempSync(path.join(os.tmpdir(), "colocated-bq-int-"));
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
         (w) => w.code === "stored-native-schema-missing",
      ),
      problems: model_?.servedCopyProblems() ?? [],
      servedBy: result.path?.servedBy,
      sql: String((result.result as unknown as { sql?: string }).sql ?? ""),
   };
}

describe("a colocated table bound with no captured warehouse types", () => {
   it("reads a BigQuery big-integer column through INT64, and serves", async () => {
      const { notices, problems, servedBy, sql } =
         await bindWithoutNativeSchema(
            modelOn("bq", "SELECT k, n FROM t"),
            new Map<string, Connection>([["bq", standInBigQuery("bq")]]),
         );
      expect(problems).toEqual([]);
      expect(servedBy?.kind).toBe("materialized");
      expect(sql).toContain("by_k_table");
      expect(sql).toMatch(/AS INT64\)/);
      expect(notices.map((w) => [w.model, w.subject, w.severity])).toEqual([
         ["m.malloy", "by_k", "warn"],
      ]);
      expect(notices[0].message).toContain("`total` is read as `INT64`");
      expect(notices[0].message).toContain("next full rebuild");
      expect(notices[0].message).not.toMatch(/file:|\//);
   });

   it("still leaves the column out on another dialect", async () => {
      const { notices, problems, servedBy } = await bindWithoutNativeSchema(
         modelOn("duckdb", "SELECT 'a' AS k, CAST(1 AS BIGINT) AS n"),
         new Map<string, Connection>(),
      );
      expect(problems.map((p) => [p.sourceName, p.reason])).toEqual([
         ["by_k", "column_types"],
      ]);
      expect(servedBy?.kind).toBe("live");
      expect(notices.map((w) => w.subject)).toEqual(["by_k"]);
      expect(notices[0].message).toContain(
         "`total` cannot be read exactly without one",
      );
      // Here the whole source answers live (`column_types`), and the
      // message does not claim only the queries reading it do.
      expect(notices[0].message).toContain("wholly live");
   });

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
