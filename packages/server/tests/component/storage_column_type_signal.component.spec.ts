// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One stored-column-type signal for every `storage=` kind: a `#@ persist`
 * source, a `#(materialize)` source, a rollup, a persisted view and a
 * persisted named query each bound over a table that stores a column as
 * another type than its build declares report the
 * `storage-column-type-mismatch` package warning (a database table's drift:
 * `database-column-type-mismatch`) and tick
 * `publisher_storage_column_type_mismatch_total{stage="serve"}`. A database
 * table whose column type the bind found moved outside a build no longer
 * matches its column contract: it is left out, `incompatible`, and warned as
 * `materialization-table-incompatible`, never cast or narrowed.
 */

import { contractFor } from "../../src/test_helpers/contract_fixture";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { resetMaterializationTelemetryForTesting } from "../../src/materialization_metrics";
import { recordConnectionsAsForTests } from "../../src/service/connection_type";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import type { ObservedColumns } from "../../src/service/materialization/domain/types";
import type { Package } from "../../src/service/package";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../src/test_helpers/metrics_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const MODEL = `##! experimental.persistence
source: raw is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'a', 1.0), (2, 'a', 2.0), (3, 'b', 4.0)) AS t(id, cat, amt)
""") extend { primary_key: id }

#@ persist storage="lake"
source: daily is raw -> { group_by: cat; aggregate: total is amt.sum() }

#(materialize) storage="lake"
source: orders is raw extend {
  measure: order_total is amt.sum()
}

source: sales is raw extend {
  #(rollup) grain=["cat"] storage="lake"
  measure: amount is amt.sum()
}

source: ops is raw extend {
  measure: n is count()
  #@ persist storage="lake"
  view: by_cat is { group_by: cat; aggregate: n }
}

#@ persist storage="lake"
query: top_cats is raw -> { group_by: cat; aggregate: c is count() }
`;

/** The DuckDB type a build stores a declared Malloy type as. */
const STORED: Record<string, string> = {
   string: "VARCHAR",
   number: "DOUBLE",
   date: "DATE",
   timestamp: "TIMESTAMP",
   boolean: "BOOLEAN",
};

let rootDir: string;
let restore: () => void;
let restoreTypes: () => void;
let harness: MetricsHarness;

beforeEach(async () => {
   restore = installSettings({
      env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
   }).restore;
   // `storage=` on the sandbox is refused; it stands in for a warehouse.
   restoreTypes = recordConnectionsAsForTests({ duckdb: "postgres" });
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-type-signal-"));
   harness = await startMetricsHarness();
   resetMaterializationTelemetryForTesting();
});

afterEach(async () => {
   resetMaterializationTelemetryForTesting();
   await harness.shutdown();
   restoreTypes();
   restore();
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

async function loadPackage(): Promise<Package> {
   const envPath = path.join(rootDir, "env");
   writePackageFiles(
      path.join(envPath, "pkg"),
      { "model.malloy": MODEL },
      { manifest: { name: "pkg", description: "fixture" } },
   );
   const env = await Environment.create("testEnv", envPath, [] as never);
   await env.addPackage("pkg");
   return env.getPackage(parsePackageName("pkg"), false);
}

/**
 * Bind every planned `storage=` table with the schema its build declares,
 * except that each table's first number column is stored as VARCHAR. Nothing
 * is queried, so no table need exist.
 */
function bindMistyped(pkg: Package): Record<string, string> {
   const stored: Record<string, Record<string, unknown>> = {};
   const mistyped: Record<string, string> = {};
   for (const source of Object.values(pkg.getBuildPlan()?.sources ?? {})) {
      const destination = source.annotationFields?.storage;
      if (!destination || stored[source.sourceEntityId]) continue;
      let changed = false;
      const schema = (source.columns ?? []).map((c) => {
         const name = c.name as string;
         if (!changed && c.type === "number") {
            changed = true;
            mistyped[source.name] = name;
            return { name, type: "VARCHAR" };
         }
         return { name, type: STORED[c.type as string] ?? "VARCHAR" };
      });
      stored[source.sourceEntityId] = {
         sourceEntityId: source.sourceEntityId,
         sourceName: source.name,
         physicalTableName: `t_${source.sourceEntityId.slice(0, 12)}`,
         contract: contractFor(),
         storageDestinationName: destination,
         ...(source.origin === "rollup" ? { origin: "rollup" } : {}),
         schema,
      };
   }
   pkg.materialization().bind({ storage: stored as never });
   return mistyped;
}

describe("a stored column whose type drifted", () => {
   it("warns and counts once for every storage= kind", async () => {
      const pkg = await loadPackage();
      const mistyped = bindMistyped(pkg);
      expect(Object.keys(mistyped)).toHaveLength(5);
      const warnings = (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === "storage-column-type-mismatch",
      );
      const bySubject = new Map(warnings.map((w) => [w.subject, w.message]));
      // A rollup is named by its base; a view or query by its build source,
      // as the package's other storage warnings name them.
      expect([...bySubject.keys()].sort()).toEqual([
         "daily",
         "ops__by_cat__view",
         "orders",
         "sales",
         "top_cats__query",
      ]);
      expect(warnings).toHaveLength(5);
      // Every reader takes the column the same way: no faithful cast reads
      // a number stored as text, so a query reading it answers live.
      for (const subject of [
         "daily",
         "ops__by_cat__view",
         "top_cats__query",
         "sales",
         "orders",
      ]) {
         expect(bySubject.get(subject)).toContain("answered live");
      }

      // Asked again, still counted once per table.
      pkg.getPackageMetadata();
      expect(
         await harness.collectCounter(
            "publisher_storage_column_type_mismatch_total",
            { stage: "serve", declared: "number" },
         ),
      ).toBe(5);
   });
});

const DATABASE_MODEL = `##! experimental.persistence
source: raw is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'a', 1.0), (2, 'a', 2.0), (3, 'b', 4.0)) AS t(id, cat, amt)
""") extend { primary_key: id }

#@ persist name="daily_t"
source: daily is raw -> { group_by: cat; aggregate: total is amt.sum() }

#(materialize) name="orders_t"
source: orders is raw extend {
  measure: order_total is amt.sum()
}

source: sales is raw extend {
  #(rollup) grain=["cat"]
  measure: amount is amt.sum()
}
`;

describe("a database table whose column type moved outside a build", () => {
   it("is left out as incompatible with its contract, and warned once per table", async () => {
      const envPath = path.join(rootDir, "env");
      writePackageFiles(
         path.join(envPath, "pkg"),
         { "model.malloy": DATABASE_MODEL },
         { manifest: { name: "pkg", description: "fixture" } },
      );
      const env = await Environment.create("testEnv", envPath, [] as never);
      await env.addPackage("pkg");
      const pkg = await env.getPackage(parsePackageName("pkg"), false);

      // Each planned database table as its build recorded it, and as the
      // bind reads it now: its first number column altered to VARCHAR.
      const entries: Record<string, Record<string, unknown>> = {};
      const observed = new Map<
         string,
         { columns: { name: string; type: string }[] }
      >();
      const moved: Record<string, string> = {};
      for (const source of Object.values(pkg.getBuildPlan()?.sources ?? {})) {
         if (entries[source.sourceEntityId]) continue;
         const schema = (source.columns ?? []).map((c) => ({
            name: c.name as string,
            type: STORED[c.type as string] ?? "VARCHAR",
         }));
         let changed = false;
         const now = schema.map((c) => {
            if (!changed && c.type === "DOUBLE") {
               changed = true;
               moved[source.name] = c.name;
               return { ...c, type: "VARCHAR" };
            }
            return c;
         });
         entries[source.sourceEntityId] = {
            sourceEntityId: source.sourceEntityId,
            sourceName: source.name,
            tableName: `t_${source.sourceEntityId.slice(0, 12)}`,
            contract: contractFor(schema),
            connectionName: "duckdb",
            schema,
         };
         observed.set(source.sourceEntityId, { columns: now });
      }
      expect(Object.keys(moved)).toHaveLength(3);
      pkg.materialization().bind({
         database: entries as never,
         databaseAsGiven: true,
         observed,
      });

      const verdicts = pkg
         .materialization()
         .serveVerdicts()
         .filter((v) => v.reason === "incompatible");
      expect(verdicts.length).toBeGreaterThanOrEqual(3);
      for (const v of verdicts) {
         expect(v.incompatibility?.code).toBe("column_retyped");
         expect(v.table?.physicalTableName).toMatch(/^t_/);
      }
      const warnings = (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === "materialization-table-incompatible",
      );
      expect(new Set(warnings.map((w) => w.subject)).size).toBe(
         warnings.length,
      );
      expect(warnings.length).toBeGreaterThanOrEqual(3);
   });
});

describe("a database table read again after a transient failure", () => {
   it("stays unread, never incompatible, until the retry reads it and binds it", async () => {
      const envPath = path.join(rootDir, "env");
      writePackageFiles(
         path.join(envPath, "pkg"),
         { "model.malloy": DATABASE_MODEL },
         { manifest: { name: "pkg", description: "fixture" } },
      );
      const env = await Environment.create("testEnv", envPath, [] as never);
      await env.addPackage("pkg");
      const pkg = await env.getPackage(parsePackageName("pkg"), false);

      // One table drifted, one unread by a transient failure, one as built.
      const entries: Record<string, Record<string, unknown>> = {};
      const asBuilt = new Map<string, { name: string; type: string }[]>();
      const observed = new Map<string, ObservedColumns>();
      const idOf: Record<string, string> = {};
      for (const source of Object.values(pkg.getBuildPlan()?.sources ?? {})) {
         const id = source.sourceEntityId;
         if (entries[id]) continue;
         idOf[source.name] = id;
         const schema = (source.columns ?? []).map((c) => ({
            name: c.name as string,
            type: STORED[c.type as string] ?? "VARCHAR",
         }));
         entries[id] = {
            sourceEntityId: id,
            sourceName: source.name,
            tableName: `t_${id.slice(0, 12)}`,
            contract: contractFor(schema),
            connectionName: "duckdb",
            schema,
         };
         asBuilt.set(id, schema);
         if (source.name === "orders") {
            observed.set(id, {
               columns: schema.map((c, i) =>
                  i === 0 ? { ...c, type: "VARCHAR" } : c,
               ),
            });
         } else if (source.name === "daily") {
            observed.set(id, { unread: "timed_out" });
         } else {
            observed.set(id, { columns: schema });
         }
      }
      const mz = pkg.materialization();
      // The retry's read succeeds, and finds the table as built.
      mz.readDatabaseColumns = async (retried) =>
         new Map(
            Object.keys(retried).map((id) => [
               id,
               { columns: asBuilt.get(id) ?? [] },
            ]),
         );
      mz.bind({
         database: entries as never,
         databaseAsGiven: true,
         observed,
      });
      const reasonOf = (name: string) =>
         mz.serveVerdicts().find((v) => v.sourceName === name)?.reason;
      expect(mz.hasUnreadDatabaseTables()).toBe(true);
      // A transient read failure is the worker's, never the table's.
      expect(reasonOf("daily")).not.toBe("incompatible");
      expect(reasonOf("orders")).toBe("incompatible");
      const deadline = Date.now() + 10_000;
      while (mz.hasUnreadDatabaseTables() && Date.now() < deadline) {
         await new Promise((r) => setTimeout(r, 100));
      }
      expect(mz.hasUnreadDatabaseTables()).toBe(false);
      expect(reasonOf("daily")).not.toBe("incompatible");
      expect(idOf.daily).toBeDefined();
   });
});
