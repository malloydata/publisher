// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Since R-13 a source's content address folds in its upstream column types,
 * so it differs from the key Malloy files its table under (its build's
 * `buildId`) for nearly every source. A manifest handed to Malloy keyed by
 * address alone is missed by Malloy's own lookup, which inlines the source's
 * raw query: the compile endpoint's `includeSql` read the base table while
 * the run read the built one. Every manifest handed to Malloy is now filed
 * under the compiler key too (`underCompilerKeys`).
 *
 * - A v1 `#@ persist` database source whose address is not its compiler
 *   key: its built table is substituted at compile, and `includeSql` names
 *   it, as the run does.
 * - A twin pair, two sources of one build placed in two tables
 *   (`name=`), folded apart: one compiler key between them. Each answer
 *   reads its own table; a compile names a table of that build, never the
 *   base table.
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
#@ persist name="v1_t"
source: v1 is warehouse.table('t') -> { group_by: cat; aggregate: total is amt.sum() }
#@ persist name="twin_a"
source: twin_a is warehouse.table('t') -> { group_by: cat; aggregate: n is count() }
#@ persist name="twin_b"
source: twin_b is warehouse.table('t') -> { group_by: cat; aggregate: n is count() }
`;

describe("a manifest handed to Malloy is filed under each table's compiler key", () => {
   let restore: () => void;
   let root: string;
   let env: Environment;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let service: MaterializationService;
   let pkg: Package;
   let run: Materialization;

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

   const executed = async (query: string) => {
      const result = await pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      return {
         sql: String((result.result as { sql?: string }).sql ?? ""),
         kind: (result.path as { servedBy?: { kind?: string } } | undefined)
            ?.servedBy?.kind,
      };
   };

   const compiled = async (query: string) =>
      (await env.compileSource(PKG, "m.malloy", query, true)).sql ?? "";

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
         fs.mkdtempSync(path.join(os.tmpdir(), "compiler-key-filing-")),
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
      run = await build();
   }, 300_000);

   afterAll(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      fs.rmSync(root, { recursive: true, force: true });
      restore();
      await freshLoadPool();
   });

   it("built every table, at addresses that are not Malloy's keys", () => {
      expect(run.status).toBe("MANIFEST_FILE_READY");
      const keys = (
         pkg.materialization() as unknown as {
            plan: { compilerKeys: Record<string, string> };
         }
      ).plan.compilerKeys;
      const sources = Object.values(pkg.getBuildPlan()?.sources ?? {});
      for (const s of sources) {
         expect({
            name: s.name,
            keyed: keys[s.sourceEntityId] !== undefined,
         }).toEqual({ name: s.name, keyed: true });
      }
      const twins = sources.filter((s) => s.name.startsWith("twin_"));
      expect(twins).toHaveLength(2);
      expect(twins[0].sourceEntityId).not.toBe(twins[1].sourceEntityId);
      expect(keys[twins[0].sourceEntityId]).toBe(keys[twins[1].sourceEntityId]);
   });

   it("substitutes the v1 table at compile, as the run reads it", async () => {
      const query = "run: v1 -> { group_by: cat, total }";
      const preview = await compiled(query);
      expect(preview).toContain("v1_t");
      expect(preview).not.toMatch(/\bt\b".*amt|SUM\(/);
      const ran = await executed(query);
      expect(ran.kind).toBe("materialized");
      expect(ran.sql).toContain("v1_t");
   });

   it("answers each twin from its own table, and compiles each to a table of their build", async () => {
      for (const twin of ["twin_a", "twin_b"]) {
         const query = `run: ${twin} -> { group_by: cat, n }`;
         const ran = await executed(query);
         expect({ twin, kind: ran.kind }).toEqual({
            twin,
            kind: "materialized",
         });
         expect(ran.sql).toContain(twin);
         const preview = await compiled(query);
         expect(preview).toMatch(/twin_[ab]/);
         expect(preview).not.toContain("COUNT(");
      }
   });
});
