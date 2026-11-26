// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * N-07: one file, two persist sources, each over its own upstream table.
 * Build both, then drop one upstream table. A worker that binds again after
 * the drop recompiles the file, and the compile cannot resolve the dropped
 * table. Before, that failure replaced the file's model with its compile
 * error, so every query on BOTH sources answered 424. Now the bind keeps
 * the file's last good model: `kept` serves its table as before, `gone`
 * stops serving its table (`incompatible`, `upstream_altered`) and answers
 * live, which fails for it alone, and a `model-recompile-failed` warning
 * names the file and its sources.
 *
 * Postgres only, when `PUBLISHER_TEST_POSTGRES_URL` names one: DuckDB's
 * read of a dropped table fails rather than reading it as gone, so a
 * DuckDB upstream keeps its last reading and is not withdrawn.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
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
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

const MODEL = `##! experimental { persistence }
#@ persist name="n07_gone"
source: gone is warehouse.table('public.up_gone') -> {
  group_by: cat
  aggregate: n is count()
}

#@ persist name="n07_kept"
source: kept is warehouse.table('public.up_kept') -> {
  group_by: cat
  aggregate: n is count()
}
`;

const Q = (s: string) =>
   `run: ${s} -> { group_by: cat, n_text is n::string; order_by: cat }`;

type Answer = { rows: unknown; kind?: string } | { error: string };

describe.skipIf(!POSTGRES_URL)(
   "N-07: an upstream table dropped, then a rebind that cannot recompile the file",
   () => {
      let restore: () => void;
      let root: string;
      let env: Environment;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let service: MaterializationService;
      let pkg: Package;
      let admin: PostgresConnection;
      const database = `n07_${process.pid}_${Date.now()}`;

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

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "off",
               PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "0",
            },
         }).restore;
         await freshLoadPool();
         admin = new PostgresConnection({
            name: "admin",
            connectionString: POSTGRES_URL,
         });
         await admin.runSQL(`CREATE DATABASE ${database}`);
         const parsed = new URL(POSTGRES_URL!);
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "upstream-table-dropped-")),
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
                  type: "postgres",
                  postgresConnection: {
                     host: parsed.hostname,
                     port: Number(parsed.port || 5432),
                     userName: decodeURIComponent(parsed.username),
                     password: decodeURIComponent(parsed.password),
                     databaseName: database,
                  },
               } as never,
            ],
            [],
         );
         const warehouse = await env.getMalloyConnection("warehouse");
         await warehouse.runSQL(
            "CREATE TABLE public.up_gone AS SELECT * FROM (VALUES ('a'), ('a'), ('b')) AS x(cat)",
         );
         await warehouse.runSQL(
            "CREATE TABLE public.up_kept AS SELECT * FROM (VALUES ('c'), ('d'), ('d')) AS x(cat)",
         );
         await env.addPackage(PKG);
         pkg = await env.getPackage(parsePackageName(PKG), false);
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
         firstRun = await build();
         before.gone = await ask(Q("gone"));
         before.kept = await ask(Q("kept"));

         await warehouse.runSQL("DROP TABLE public.up_gone");
         // A worker that binds after the drop holds no schema it read
         // before (the load pool caches none here, and the connection's own
         // cache is emptied), so the recompile reads the dropped table.
         (warehouse as unknown as { schemaCache: object }).schemaCache = {};
         await pkg.reloadAllModels(
            pkg.materialization().boundDatabaseEntries() ?? {},
         );
         pkg = await env.getPackage(parsePackageName(PKG), false);
         after.gone = await ask(Q("gone"));
         after.kept = await ask(Q("kept"));
      }, 300_000);

      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         if (root) fs.rmSync(root, { recursive: true, force: true });
         await admin
            ?.runSQL(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`)
            .catch(() => undefined);
         await admin?.close();
         restore?.();
         await freshLoadPool();
      });

      it("built and served both tables before the drop", () => {
         expect(firstRun.status).toBe("MANIFEST_FILE_READY");
         expect(before.gone).toMatchObject({ kind: "materialized" });
         expect(before.kept).toMatchObject({ kind: "materialized" });
      });

      it("keeps the file's last good model: kept serves its table, no 424", () => {
         expect(
            pkg.getModel("m.malloy")!.getCompilationError(),
         ).toBeUndefined();
         expect(after.kept).toEqual({
            rows: [
               { cat: "c", n_text: "1" },
               { cat: "d", n_text: "2" },
            ],
            kind: "materialized",
         });
      });

      it("answers gone live, which fails for it alone, never from its old table", () => {
         expect("error" in after.gone).toBe(true);
         expect((after.gone as { error: string }).error).not.toContain(
            "compiling model",
         );
      });

      it("reads gone as incompatible, its upstream altered", () => {
         const verdicts = pkg
            .materialization()
            .serveVerdicts()
            .filter((v) => v.reason === "incompatible");
         expect(
            verdicts.map((v) => [
               "sourceName" in v ? v.sourceName : undefined,
               v.incompatibility?.code,
            ]),
         ).toEqual([["gone", "upstream_altered"]]);
      });

      it("warns model-recompile-failed, naming the file and its sources", () => {
         const warnings = (pkg.getPackageMetadata().warnings ?? []).filter(
            (w) => w.code === MODEL_RECOMPILE_FAILED,
         );
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toMatchObject({
            model: "m.malloy",
            reason: "compile_failed",
            severity: "warn",
         });
         expect(warnings[0].message).toContain("its sources (gone, kept)");
         expect(warnings[0].message).toContain("gone answer live");
      });
   },
);
