// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database persisted view or named query over a database `#@ persist`
 * source builds from that source's table, as any chained build does: its
 * entry says `upstreamReuse: "reused"`, it builds under strict upstreams,
 * and no upstream recompute is counted. Its stored rows answer as live does,
 * as text. (The drop-raw proof is downstream_reads_upstream_table.)
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
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
import { resetMaterializationTelemetryForTesting } from "../../src/materialization_metrics";
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
    ('b', CAST(2.4 AS DECIMAL(10,1)))
  ) AS t(cat, amt)
""")

#@ persist name="pr_a"
source: a is raw -> { select: cat, amt }

source: s is a extend {
  #@ persist name="pr_v"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}

#@ persist name="pr_q"
query: q is a -> { group_by: cat; aggregate: total is amt.sum() }

source: live is raw -> { group_by: cat; aggregate: total is amt.sum() }
`;

const AS_TEXT =
   "-> { group_by: cat, total_text is total::string; order_by: cat }";

interface Entry {
   sourceName?: string;
   upstreamReuse?: string;
   upstreamRecomputeReason?: string;
}

describe("a persisted view or query over a persisted source reads its table", () => {
   let restore: () => void;
   let root: string;
   let env: Environment;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let service: MaterializationService;
   let pkg: Package;
   let strict: Materialization;
   let relaxed: Materialization;
   let metrics: MetricsHarness;
   let recomputes: number;

   const ask = async (query: string) => {
      const result = await pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      return {
         rows: JSON.parse(JSON.stringify(result.compactResult)),
         kind: (result.path as { servedBy?: { kind?: string } } | undefined)
            ?.servedBy?.kind,
      };
   };
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
   const results = (m: Materialization): Entry[] =>
      Object.values(
         (m.manifest as { entries?: Record<string, Entry> }).entries ?? {},
      ).filter((e) => e.sourceName !== "a");

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
      root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pr-")));
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
      // `a` first, so each result has a stored upstream to (not) read.
      await build({ sourceNames: ["a"] });
      resetMaterializationTelemetryForTesting();
      strict = await build({});
      recomputes = await metrics.collectCounter(
         "publisher_materialization_upstream_recompute_total",
         {
            reason: "persisted_result",
            build: "chained",
            outcome: "recomputed",
         },
      );
      // Rebuilt, so the relaxed run's entries are its own.

      relaxed = await build({ forceRefresh: true });
   }, 300_000);

   afterAll(async () => {
      await metrics?.shutdown();
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      fs.rmSync(root, { recursive: true, force: true });
      restore();
      await freshLoadPool();
   });

   it("builds each under strict upstreams, reusing the source's table, and counts no recompute", () => {
      expect([strict.status, (strict as { error?: unknown }).error]).toEqual([
         "MANIFEST_FILE_READY",
         null,
      ]);
      const entries = results(strict);
      expect(entries).toHaveLength(2);
      for (const e of entries) {
         expect(e.upstreamReuse).toBe("reused");
         expect(e.upstreamRecomputeReason).toBeUndefined();
      }
      expect(recomputes).toBe(0);
   });

   it("records each as reused on a relaxed rebuild too", () => {
      expect(relaxed.status).toBe("MANIFEST_FILE_READY");
      const entries = results(relaxed);
      expect(entries).toHaveLength(2);
      for (const e of entries) expect(e.upstreamReuse).toBe("reused");
   });

   it("serves the view from its table, equal to live as text", async () => {
      const served = await ask(`run: s -> by_cat ${AS_TEXT}`);
      expect(served.kind).toBe("view");
      expect(served.rows).toEqual((await ask(`run: live ${AS_TEXT}`)).rows);
   });
});
