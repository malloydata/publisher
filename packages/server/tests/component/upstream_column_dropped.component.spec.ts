// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * G2 (campaign P9-01): an upstream column dropped, so the explicit reload
 * that refetches it fails to compile and the previous model keeps serving.
 * Before, that model kept answering from the tables built over the dropped
 * column as fresh, while live answered 400. Now the failed reload reads the
 * tables again: `by_cat`, whose build reads the dropped `amt`, stops serving
 * its table and answers as live does (the same error); `by_n`, a count over
 * the same table that names no dropped column, keeps serving, equal to live
 * as text; the package's stale status says which sources answer live.
 *
 * The R-13 note follows for the shared harness:
 * R-13: a column retyped upstream of a persisted source, whose SQL does not
 * change, re-addresses the source on the next explicit reload, so the next
 * run rebuilds its table instead of carrying the one built from the old
 * column; until then the query answers live. Two retypes: INTEGER to DOUBLE
 * with the values moved (the dev repro: INT64 to FLOAT64, +0.5), and
 * DECIMAL(10,2) to DECIMAL(12,4), which Malloy's own types (both `number`)
 * do not tell apart. A reload over an unchanged table keeps every address.
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
#@ persist
source: by_cat is warehouse.table('t') -> { group_by: cat; aggregate: total is amt.sum() }
#@ persist
source: by_n is warehouse.table('t') -> { group_by: cat; aggregate: n is count() }

#@ -persist
source: by_cat__live is by_cat extend {}
#@ -persist
source: by_n__live is by_n extend {}
`;

const CAT = (s: string) =>
   `run: ${s} -> { group_by: cat, total_text is total::string; order_by: cat }`;
const N = (s: string) =>
   `run: ${s} -> { group_by: cat, n_text is n::string; order_by: cat }`;

type Answer = { rows: unknown; kind?: string } | { error: string };

describe("G2: an upstream column dropped under a failed reload", () => {
   let restore: () => void;
   let root: string;
   let env: Environment;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let service: MaterializationService;
   let pkg: Package;

   const ask = async (query: string): Promise<Answer> => {
      try {
         const result = await pkg
            .getModel("m.malloy")!
            .runQuery(
               unrecordedAudit(),
               { query } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         const report = result.path as
            | { servedBy?: { kind?: string } }
            | undefined;
         return {
            rows: JSON.parse(
               JSON.stringify(result.compactResult, (_k, v) =>
                  typeof v === "bigint" ? `${v}` : (v as unknown),
               ),
            ),
            kind: report?.servedBy?.kind,
         };
      } catch (e) {
         return { error: (e as Error).message };
      }
   };

   const build = async (): Promise<Materialization> => {
      const created = await service.createMaterialization(ENV, PKG, {});
      const running = (
         service as unknown as { runningAbortControllers: Map<string, unknown> }
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

   let firstRun: Materialization;
   let reloadError = "";
   const before: Record<string, Answer> = {};
   const after: Record<string, Answer> = {};

   beforeAll(async () => {
      restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "off",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      await freshLoadPool();
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "upstream-dropped-")),
      );
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
      const warehouse = await env.getMalloyConnection("warehouse");
      await warehouse.runSQL(
         "CREATE TABLE t AS SELECT cat, amt::INTEGER AS amt FROM (VALUES ('a', 1), ('a', 2), ('b', 5)) AS x(cat, amt)",
      );
      await env.addPackage(PKG);
      pkg = await env.getPackage(parsePackageName(PKG), false);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
      firstRun = await build();
      before.by_cat = await ask(CAT("by_cat"));
      before.by_n = await ask(N("by_n"));

      await warehouse.runSQL("ALTER TABLE t DROP COLUMN amt");
      try {
         await env.getPackage(parsePackageName(PKG), true, {
            refreshSchemas: true,
         });
      } catch (e) {
         reloadError = (e as Error).message;
      }
      // The previous copy keeps serving.
      pkg = await env.getPackage(parsePackageName(PKG), false);
      after.by_cat = await ask(CAT("by_cat"));
      after.by_cat__live = await ask(CAT("by_cat__live"));
      after.by_n = await ask(N("by_n"));
      after.by_n__live = await ask(N("by_n__live"));
   }, 300_000);

   afterAll(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      fs.rmSync(root, { recursive: true, force: true });
      restore();
      await freshLoadPool();
   });

   it("built both tables and served both before the drop", () => {
      expect(firstRun.status).toBe("MANIFEST_FILE_READY");
      expect(before.by_cat).toMatchObject({ kind: "materialized" });
      expect(before.by_n).toMatchObject({ kind: "materialized" });
   });

   it("failed the reload, which keeps the previous model serving", () => {
      expect(reloadError).toContain("amt");
   });

   it("answers by_cat as live does, the same error, never from its old table", () => {
      expect("error" in after.by_cat__live).toBe(true);
      expect(after.by_cat).toEqual(after.by_cat__live);
   });

   it("keeps serving by_n, whose build names no dropped column, equal to live as text", () => {
      expect(after.by_n).toMatchObject({ kind: "materialized" });
      expect(after.by_n__live).toMatchObject({ kind: "live" });
      expect("rows" in after.by_n && after.by_n.rows).toEqual(
         "rows" in after.by_n__live && after.by_n__live.rows,
      );
      expect("rows" in after.by_n && after.by_n.rows).toEqual([
         { cat: "a", n_text: "2" },
         { cat: "b", n_text: "1" },
      ]);
   });

   it("says in the package's stale status which sources answer live, and why", () => {
      const stale = env.getStaleCompileErrors().get(PKG);
      expect(stale?.message).toContain("by_cat answer live");
      expect(stale?.message).not.toContain("by_n");
   });
});
