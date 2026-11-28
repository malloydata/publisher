// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * N-07, the environment's paths: a failed reload (refetching the schemas)
 * and a failed publish keep the previous package serving, and withdraw the
 * stored tables whose upstream they could not read (`upstream_unread`),
 * held across plan derivations. This package keeps no file of its own, so
 * the hold must not be released because of that:
 *
 * - a manifest rebind with the read still failing keeps the hold, so a plan
 *   refresh afterwards (a connection edit's, which derives a plan that
 *   would address `side` at its old table) still answers `side` live,
 *   equal to live, never from its last build;
 * - a rebind that recompiles every file while the read still fails keeps
 *   it withdrawn and held, with a verdict that no longer says the model
 *   did not recompile;
 * - once the read succeeds and finds `up_side` unchanged, a rebind serves
 *   it again.
 *
 * DuckDB, in process: the read's failure is injected on the warehouse
 * connection's `DESCRIBE` of `up_side` alone. The reload is failed by a
 * broken file, the publish by its validator.
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
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const PKG = "pkg";

const MODEL = `##! experimental { persistence }
#@ persist name="n07h_s"
source: side is warehouse.table('up_side') -> {
  group_by: cat
  aggregate: n is count()
}

source: side_live is warehouse.table('up_side') -> {
  group_by: cat
  aggregate: n is count()
}
`;

const Q = (s: string) =>
   `run: ${s} -> { group_by: cat, n_text is n::string; order_by: cat }`;

type Answer = { rows: unknown; kind?: string } | { error: string };

for (const failure of ["reload", "publish"] as const) {
   describe(`N-07: a failed ${failure} holds an unread upstream's withdrawal across rebinds`, () => {
      let restore: () => void;
      let root: string;
      let envPath: string;
      let env: Environment;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let service: MaterializationService;
      let pkg: Package;
      const fault = { on: false };
      let failedWith = "";
      let served: Package;

      const ask = async (query: string): Promise<Answer> => {
         try {
            const result = await pkg
               .getModel("m.malloy")!
               .runQuery(
                  unrecordedAudit(),
                  { query } as never,
                  QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
               );
            return {
               rows: result.compactResult,
               kind: result.path?.servedBy?.kind,
            };
         } catch (e) {
            return { error: (e as Error).message };
         }
      };

      const verdictOf = (name: string) =>
         pkg
            .materialization()
            .serveVerdicts()
            .find((v) => "sourceName" in v && v.sourceName === name);

      /** `side` answers live, equal to `side_live`, never its last build. */
      const expectSideLive = async () => {
         const side = await ask(Q("side"));
         const live = await ask(Q("side_live"));
         expect(live).toMatchObject({
            rows: [
               { cat: "e", n_text: "2" },
               { cat: "f", n_text: "1" },
               { cat: "g", n_text: "1" },
            ],
         });
         expect(side).not.toMatchObject({ kind: "materialized" });
         expect((side as { rows: unknown }).rows).toEqual(
            (live as { rows: unknown }).rows,
         );
      };

      const rebind = (ifChanged: boolean) =>
         pkg.reloadAllModels(
            pkg.materialization().boundDatabaseEntries() ?? {},
            { ifChanged },
         );

      const refreshPlan = async () => {
         pkg.markBuildPlanStale();
         expect(await pkg.refreshBuildPlan()).toBe(true);
      };

      const build = async (): Promise<Materialization> => {
         const created = await service.createMaterialization(ENV, PKG, {});
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         const deadline = Date.now() + 120_000;
         for (;;) {
            const m = await repo.getMaterializationById(created.id);
            if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
               while (running.has(created.id)) {
                  await new Promise((resolve) => setTimeout(resolve, 10));
               }
               return m;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((resolve) => setTimeout(resolve, 20));
         }
      };

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "off",
               PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "0",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await freshLoadPool();
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "upstream-env-hold-")),
         );
         envPath = path.join(root, "env");
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
         const warehouse = await env.getMalloyConnection("warehouse");
         await warehouse.runSQL(
            "CREATE TABLE up_side AS SELECT * FROM (VALUES ('e'), ('f')) AS x(cat)",
         );
         await env.addPackage(PKG);
         pkg = await env.getPackage(parsePackageName(PKG), false);
         const conn = (await pkg.getMalloyConnection(
            "warehouse",
         )) as unknown as {
            runSQL: (sql: string, options?: unknown) => Promise<unknown>;
         };
         const runSQL = conn.runSQL.bind(conn);
         conn.runSQL = (sql, options) =>
            fault.on && /^\s*DESCRIBE\b/i.test(sql) && sql.includes("up_side")
               ? Promise.reject(new Error("catalog unavailable"))
               : runSQL(sql, options);
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
         const run = await build();
         expect(run.status).toBe("MANIFEST_FILE_READY");
         expect(await ask(Q("side"))).toMatchObject({ kind: "materialized" });

         // Rows the last build of `side` does not hold.
         await warehouse.runSQL("INSERT INTO up_side VALUES ('e'), ('g')");
         fault.on = true;
         if (failure === "reload") {
            const broken = path.join(envPath, PKG, "broken.malloy");
            fs.writeFileSync(broken, "source: nope is \n");
            try {
               await env.getPackage(parsePackageName(PKG), true, {
                  refreshSchemas: true,
               });
            } catch (e) {
               failedWith = (e as Error).message;
            }
            fs.rmSync(broken);
         } else {
            try {
               await env.installPackage(
                  PKG,
                  async (staging) =>
                     writePackageFiles(
                        staging,
                        { "m.malloy": MODEL },
                        { manifest: { name: PKG, description: "fixture" } },
                     ),
                  () => "refused by the test's validator",
               );
            } catch (e) {
               failedWith = (e as Error).message;
            }
         }
         served = await env.getPackage(parsePackageName(PKG), false);
      }, 300_000);

      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         if (root) fs.rmSync(root, { recursive: true, force: true });
         restore?.();
         await freshLoadPool();
      });

      it(`failed the ${failure}, and keeps the previous package serving`, () => {
         expect(failedWith).not.toBe("");
         expect(served).toBe(pkg);
      });

      it("withdrew side as upstream_unread, answered live", async () => {
         expect(verdictOf("side")).toMatchObject({
            verdict: "live",
            reason: "upstream_unread",
         });
         await expectSideLive();
      });

      it("keeps side live across a manifest rebind and a plan refresh while the read fails", async () => {
         // The first may recompile; the second binds an unchanged manifest
         // without recompiling, and reads the upstream again.
         await rebind(true);
         expect(verdictOf("side")).toMatchObject({ reason: "upstream_unread" });
         expect(await rebind(true)).toBe(false);
         expect(verdictOf("side")).toMatchObject({ reason: "upstream_unread" });
         await refreshPlan();
         expect(verdictOf("side")).toMatchObject({ reason: "upstream_unread" });
         await expectSideLive();
      });

      it("keeps side live and held once every file compiles again, while the read fails", async () => {
         await rebind(false);
         const side = verdictOf("side");
         expect(side).toMatchObject({ reason: "upstream_unread" });
         expect(side?.message).not.toContain("recompile");
         await refreshPlan();
         expect(verdictOf("side")).toMatchObject({ reason: "upstream_unread" });
         await expectSideLive();
      });

      it("serves side again at the next rebind once a read finds up_side unchanged", async () => {
         fault.on = false;
         await rebind(true);
         expect(verdictOf("side")).toMatchObject({ verdict: "serves" });
         expect(await ask(Q("side"))).toMatchObject({ kind: "materialized" });
         // Released: a plan refresh keeps it serving.
         await refreshPlan();
         expect(verdictOf("side")).toMatchObject({ verdict: "serves" });
      });
   });
}
