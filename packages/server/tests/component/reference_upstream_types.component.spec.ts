// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A host's reference to an upstream table is checked against the
 * `nativeSchema` it carries before a build reads the table in place of its
 * source. With the table altered since its build (`DECIMAL(10,1)` to
 * `DECIMAL(10,0)`, which rounds `2.4` to `2`), every build is refused with
 * SourceFailure code `schema_incompatible`, never recomputing the upstream
 * from raw. A reference that carries no types cannot be checked: a build
 * reads it as before,
 * with a warning and a count, and the rows it stores are the table's.
 */

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { logger } from "../../src/logger";
import { resetMaterializationTelemetryForTesting } from "../../src/materialization_metrics";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../src/test_helpers/metrics_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const PKG = "pkg";

const MODEL = `##! experimental { persistence }
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES
    ('a', CAST(9.0 AS DECIMAL(10,1))), ('a', CAST(10.0 AS DECIMAL(10,1))),
    ('b', CAST(2.4 AS DECIMAL(10,1))), ('b', CAST(2.4 AS DECIMAL(10,1)))
  ) AS t(cat, amt)
""")

#@ persist name="rt_a"
source: a is raw -> { select: cat, amt }

#@ persist name="rt_b"
source: b is a -> { group_by: cat; aggregate: total is amt.sum() }
`;

interface Entry {
   sourceEntityId: string;
   sourceName?: string;
   physicalTableName: string;
   nativeSchema?: { name: string; type: string }[];
   upstreamReuse?: string;
   upstreamRecomputeReason?: string;
}

describe("a host's reference is checked against the nativeSchema it carries", () => {
   let restore: () => void;
   let root: string;
   let env: Environment;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let service: MaterializationService;
   let pkg: Package;
   let metrics: MetricsHarness;
   let upstream: Entry;
   let typedStrict: Materialization;
   let typedRelaxed: Materialization;
   let untypedStrict: Materialization;
   let untypedWarnings: unknown[][];
   let untypedCount: number;

   const build = async (opts: object): Promise<Materialization> => {
      const created = await service.createMaterialization(ENV, PKG, opts);
      const running = (
         service as unknown as { runningAbortControllers: Map<string, unknown> }
      ).runningAbortControllers;
      const deadline = Date.now() + 120_000;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
            while (running.has(created.id)) {
               await new Promise((r) => setTimeout(r, 10));
            }
            return (await repo.getMaterializationById(created.id)) ?? m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((r) => setTimeout(r, 20));
      }
   };
   /** `b` alone, as a host instructs it, with `a` by reference. */
   const buildB = (table: string, reference: Partial<Entry>) => {
      const b = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
         (s) => s.name === "b",
      )!;
      return build({
         buildInstructions: [
            {
               sourceEntityId: b.sourceEntityId,
               sourceID: b.sourceID,
               materializedTableId: `mt-${table}`,
               physicalTableName: table,
               realization: "COPY",
            },
         ],
         referenceManifest: [
            {
               sourceEntityId: upstream.sourceEntityId,
               physicalTableName: upstream.physicalTableName,
               connectionName: "warehouse",
               ...reference,
            },
         ],
      });
   };
   const stored = async (table: string) =>
      (
         await (
            await env.getMalloyConnection("warehouse")
         ).runSQL(
            `SELECT cat, total::VARCHAR AS total FROM ${table} ORDER BY cat`,
         )
      ).rows;

   beforeAll(async () => {
      restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "off",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      await freshLoadPool();
      metrics = await startMetricsHarness();
      root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "rt-")));
      const envPath = path.join(root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(
         path.join(envPath, PKG),
         { "m.malloy": MODEL },
         { manifest: { name: PKG, description: "fixture" } },
      );
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         ["env-1", ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      env = await Environment.create(
         ENV,
         envPath,
         [
            {
               name: "warehouse",
               type: "duckdb",
               duckdbConnection: { setupSQL: "SELECT 1" },
            } as never,
         ],
         [],
      );
      await env.addPackage(PKG);
      pkg = await env.getPackage(parsePackageName(PKG), false);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
      const first = await build({ sourceNames: ["a"] });
      upstream = Object.values(
         (first.manifest as { entries: Record<string, Entry> }).entries,
      )[0];
      // Out of band: the warehouse's own ALTER of `a`'s table.
      await (
         await env.getMalloyConnection("warehouse")
      ).runSQL(
         `ALTER TABLE ${upstream.physicalTableName} ALTER amt TYPE DECIMAL(10,0)`,
      );
      const typed = { nativeSchema: upstream.nativeSchema };
      typedStrict = await buildB("rt_b_strict", typed);
      typedRelaxed = await buildB("rt_b_relaxed", typed);
      // No run on record holds `a`'s types either, as for a host whose
      // worker never built it: the reference is all there is.
      for (const run of [first, typedStrict, typedRelaxed]) {
         await repo.deleteMaterialization(run.id);
      }
      resetMaterializationTelemetryForTesting();
      const warn = spyOn(logger, "warn");
      try {
         untypedStrict = await buildB("rt_b_untyped", {});
         untypedWarnings = warn.mock.calls.filter((c) =>
            String(c[0]).includes(
               "reads a referenced upstream table unchecked",
            ),
         );
      } finally {
         warn.mockRestore();
      }
      untypedCount = await metrics.collectCounter(
         "publisher_materialization_upstream_recompute_total",
         {
            reason: "reference_types_unrecorded",
            build: "chained",
            outcome: "read_unchecked",
         },
      );
   }, 300_000);

   afterAll(async () => {
      await metrics?.shutdown();
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      fs.rmSync(root, { recursive: true, force: true });
      restore();
      await freshLoadPool();
   });

   it("recorded the upstream's native types", () => {
      expect(upstream.nativeSchema).toEqual([
         { name: "cat", type: "VARCHAR" },
         { name: "amt", type: "DECIMAL(10,1)" },
      ]);
   });

   it("refuses every build over a typed reference whose table moved, strict or not", () => {
      for (const run of [typedStrict, typedRelaxed]) {
         expect(run.status).toBe("FAILED");
         const failures = Object.values(
            (
               run.manifest as {
                  failures?: Record<string, { reason: string; code?: string }>;
               }
            ).failures ?? {},
         );
         expect(failures).toHaveLength(1);
         expect(failures[0].code).toBe("schema_incompatible");
         expect(failures[0].reason).toContain(
            "persisted upstream 'a' was not read from its table, since its column 'amt' was built as DECIMAL(10,1) and is now DECIMAL(10,0)",
         );
      }
   });

   it("reads an untyped reference unchecked under strict, warning and counting it", async () => {
      expect(untypedStrict.status).toBe("MANIFEST_FILE_READY");
      expect(untypedWarnings).toHaveLength(1);
      expect(untypedCount).toBe(1);
      // What it read is the table as altered: the reason to send types.
      expect(await stored("rt_b_untyped")).toEqual([
         { cat: "a", total: "19" },
         { cat: "b", total: "4" },
      ]);
   });
});
