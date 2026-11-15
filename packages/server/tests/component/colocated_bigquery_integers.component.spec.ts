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
      runSQL: async () => ({ rows: [], totalRows: 0 }),
   });
   return connection;
}

const modelOn = (
   connection: string,
   sql: string,
) => `##! experimental { persistence }

#@ persist name="by_k"
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
   await pkg.reloadAllModels({
      [source.sourceEntityId]: {
         tableName: "scratch.by_k_table",
         connectionName: source.connectionName,
         freshnessWindowSeconds: 86400,
         freshnessFallback: "live",
         dataAsOf: new Date().toISOString(),
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
      problems: model_?.servedCopyProblems() ?? [],
      servedBy: result.path?.servedBy,
      sql: String((result.result as unknown as { sql?: string }).sql ?? ""),
   };
}

describe("a colocated table bound with no captured warehouse types", () => {
   it("reads a BigQuery big-integer column through INT64, and serves", async () => {
      const { problems, servedBy, sql } = await bindWithoutNativeSchema(
         modelOn("bq", "SELECT k, n FROM t"),
         new Map<string, Connection>([["bq", standInBigQuery("bq")]]),
      );
      expect(problems).toEqual([]);
      expect(servedBy?.kind).toBe("materialized");
      expect(sql).toContain("by_k_table");
      expect(sql).toMatch(/AS INT64\)/);
   });

   it("still leaves the column out on another dialect", async () => {
      const { problems, servedBy } = await bindWithoutNativeSchema(
         modelOn("duckdb", "SELECT 'a' AS k, CAST(1 AS BIGINT) AS n"),
         new Map<string, Connection>(),
      );
      expect(problems.map((p) => [p.sourceName, p.reason])).toEqual([
         ["by_k", "column_types"],
      ]);
      expect(servedBy?.kind).toBe("live");
   });
});
