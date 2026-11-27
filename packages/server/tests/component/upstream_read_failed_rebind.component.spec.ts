// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * N-07, when the upstream read fails: one file, three persist sources, each
 * over its own upstream table. Build all three, then drop `up_gone` (so a
 * rebind cannot recompile the file, and keeps its last good model), add
 * rows to `up_side`, and make every catalog read of `up_side` fail.
 *
 * Before, a failed read counted as unchanged: `side` kept serving its last
 * build. Now it is not known to be unchanged, so:
 * - `side` stops serving its table (`upstream_unread`) and answers live,
 *   rows equal to live (the rows added after the build included);
 * - `gone` still reads `incompatible` (`upstream_altered`), `kept` still
 *   serves: only the unconfirmed source is withdrawn;
 * - the `model-recompile-failed` warning says `up_side` could not be read;
 * - a plan derivation in the meantime does not hand `side` back;
 * - once a read succeeds and finds `up_side` unchanged, the next rebind
 *   (an unchanged manifest's) serves `side` from its table again;
 * - a withdrawal that throws before it can tell which tables withdraws
 *   every stored table of the kept file, as unread.
 *
 * DuckDB, in process: the read's failure is injected on the warehouse
 * connection's `DESCRIBE` of `up_side` alone.
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
import {
   MODEL_RECOMPILE_FAILED,
   type Package,
} from "../../src/service/package";
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
#@ persist name="n07b_g"
source: gone is warehouse.table('up_gone') -> {
  group_by: cat
  aggregate: n is count()
}

#@ persist name="n07b_k"
source: kept is warehouse.table('up_kept') -> {
  group_by: cat
  aggregate: n is count()
}

#@ persist name="n07b_s"
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

/** `side` as a source that reads no table answers it: live. */
const LIVE_SIDE = Q("side_live");

type Answer = { rows: unknown; kind?: string } | { error: string };

describe("N-07: a kept file whose upstream read fails answers live, until a read succeeds", () => {
   let restore: () => void;
   let root: string;
   let env: Environment;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let service: MaterializationService;
   let pkg: Package;
   const fault = { on: false };

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

   const recompileWarning = () =>
      (pkg.getPackageMetadata().warnings ?? []).find(
         (w) => w.code === MODEL_RECOMPILE_FAILED,
      );

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

   let firstRun: Materialization;
   const before: Record<string, Answer> = {};
   const after: Record<string, Answer> = {};
   let liveSide: Answer;

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
         fs.mkdtempSync(path.join(os.tmpdir(), "upstream-read-failed-")),
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
         "CREATE TABLE up_gone AS SELECT * FROM (VALUES ('a'), ('a'), ('b')) AS x(cat)",
      );
      await warehouse.runSQL(
         "CREATE TABLE up_kept AS SELECT * FROM (VALUES ('c'), ('d'), ('d')) AS x(cat)",
      );
      await warehouse.runSQL(
         "CREATE TABLE up_side AS SELECT * FROM (VALUES ('e'), ('f')) AS x(cat)",
      );
      await env.addPackage(PKG);
      pkg = await env.getPackage(parsePackageName(PKG), false);
      // Every catalog read of `up_side` fails while `fault.on`; its queries
      // do not.
      const conn = (await pkg.getMalloyConnection("warehouse")) as unknown as {
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
      firstRun = await build();
      before.gone = await ask(Q("gone"));
      before.kept = await ask(Q("kept"));
      before.side = await ask(Q("side"));

      await warehouse.runSQL("DROP TABLE up_gone");
      // Rows the last build of `side` does not hold: an answer from its
      // table would miss them.
      await warehouse.runSQL("INSERT INTO up_side VALUES ('e'), ('g')");
      (warehouse as unknown as { schemaCache: object }).schemaCache = {};
      fault.on = true;
      await pkg.reloadAllModels(
         pkg.materialization().boundDatabaseEntries() ?? {},
      );
      pkg = await env.getPackage(parsePackageName(PKG), false);
      after.gone = await ask(Q("gone"));
      after.kept = await ask(Q("kept"));
      after.side = await ask(Q("side"));
      liveSide = await ask(LIVE_SIDE);
   }, 300_000);

   afterAll(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      if (root) fs.rmSync(root, { recursive: true, force: true });
      restore?.();
      await freshLoadPool();
   });

   it("built and served all three tables before the drop", () => {
      expect(firstRun.status).toBe("MANIFEST_FILE_READY");
      expect(before.gone).toMatchObject({ kind: "materialized" });
      expect(before.kept).toMatchObject({ kind: "materialized" });
      expect(before.side).toMatchObject({ kind: "materialized" });
   });

   it("keeps the file's last good model", () => {
      expect(pkg.getModel("m.malloy")!.getCompilationError()).toBeUndefined();
   });

   it("answers side live, equal to live, never from its last build", () => {
      expect(liveSide).toMatchObject({
         rows: [
            { cat: "e", n_text: "2" },
            { cat: "f", n_text: "1" },
            { cat: "g", n_text: "1" },
         ],
      });
      expect(after.side).not.toMatchObject({ kind: "materialized" });
      expect((after.side as { rows: unknown }).rows).toEqual(
         (liveSide as { rows: unknown }).rows,
      );
   });

   it("reads side as upstream_unread, not as changed", () => {
      const side = verdictOf("side");
      expect(side).toMatchObject({
         verdict: "live",
         reason: "upstream_unread",
      });
      expect(side?.incompatibility).toBeUndefined();
      expect(side?.message).toContain("is not known");
   });

   it("withdraws only the unconfirmed source: gone stays upstream_altered, kept serves", () => {
      expect(verdictOf("gone")).toMatchObject({
         verdict: "live",
         reason: "incompatible",
         incompatibility: { code: "upstream_altered" },
      });
      expect(verdictOf("kept")).toMatchObject({ verdict: "serves" });
      expect(after.kept).toEqual({
         rows: [
            { cat: "c", n_text: "1" },
            { cat: "d", n_text: "2" },
         ],
         kind: "materialized",
      });
      expect("error" in after.gone).toBe(true);
   });

   it("warns model-recompile-failed, naming the table it could not read", () => {
      const message = recompileWarning()?.message ?? "";
      expect(message).toContain("its sources (gone, kept, side, side_live)");
      expect(message).toContain("Upstream table up_gone changed");
      expect(message).toContain("Upstream table up_side could not be read");
      expect(message).toContain("side answer live until a read");
   });

   it("serves side again at the next rebind once a read finds up_side unchanged", async () => {
      fault.on = false;
      await pkg.reloadAllModels(
         pkg.materialization().boundDatabaseEntries() ?? {},
         { ifChanged: true },
      );
      expect(verdictOf("side")).toMatchObject({ verdict: "serves" });
      expect(await ask(Q("side"))).toMatchObject({ kind: "materialized" });
      // gone's upstream did change: it stays withdrawn, as changed.
      expect(verdictOf("gone")).toMatchObject({
         reason: "incompatible",
         incompatibility: { code: "upstream_altered" },
      });
      const message = recompileWarning()?.message ?? "";
      expect(message).not.toContain("could not be read");
      expect(message).toContain("Upstream table up_gone changed");
   });

   it("withdraws every stored table of the kept file when the read cannot tell which", async () => {
      (
         pkg as unknown as { readChangedUpstreams: () => Promise<never> }
      ).readChangedUpstreams = () => Promise.reject(new Error("boom"));
      const withdrawn = await pkg.withdrawChangedUpstreams({
         modelPaths: ["m.malloy"],
      });
      expect(withdrawn.unread).toEqual({
         sources: ["kept", "side"],
         tables: undefined,
      });
      expect(verdictOf("kept")).toMatchObject({ reason: "upstream_unread" });
      expect(verdictOf("side")).toMatchObject({ reason: "upstream_unread" });
      expect(verdictOf("gone")).toMatchObject({ reason: "incompatible" });
      expect(await ask(Q("kept"))).not.toMatchObject({ kind: "materialized" });
   });

   it("keeps side withdrawn across a plan derivation while the read fails", async () => {
      delete (pkg as unknown as { readChangedUpstreams?: unknown })
         .readChangedUpstreams;
      fault.on = true;
      await pkg.reloadAllModels(
         pkg.materialization().boundDatabaseEntries() ?? {},
      );
      expect(verdictOf("kept")).toMatchObject({ verdict: "serves" });
      expect(verdictOf("side")).toMatchObject({ reason: "upstream_unread" });
      pkg.markBuildPlanStale();
      await pkg.refreshBuildPlan();
      // Never from its old table: it still answers live, equal to live.
      const side = await ask(Q("side"));
      expect(side).not.toMatchObject({ kind: "materialized" });
      expect((side as { rows: unknown }).rows).toEqual(
         (liveSide as { rows: unknown }).rows,
      );
   });
});
