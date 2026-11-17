// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One stored-column-type signal for every `storage=` kind: a `#@ persist`
 * source, a `#(materialize)` source, a rollup, a persisted view and a
 * persisted named query each bound over a table that stores a column as
 * another type than its build declares report the
 * `storage-column-type-mismatch` package warning and tick
 * `publisher_storage_column_type_mismatch_total{stage="serve"}`.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { resetMaterializationTelemetryForTesting } from "../../src/materialization_metrics";
import { recordConnectionsAsForTests } from "../../src/service/connection_type";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
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
  #(preaggregate) grain=["cat"] storage="lake"
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
      env: {
         PUBLISHER_PERSIST_STORAGE_MODE: "on",
         PUBLISHER_MATERIALIZE_ENABLED: "true",
      },
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
         storageDestinationName: destination,
         ...(source.origin === "preaggregate"
            ? { origin: "preaggregate" }
            : {}),
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
