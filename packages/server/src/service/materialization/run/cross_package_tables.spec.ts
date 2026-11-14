// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../access_record";
import type {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../../storage/duckdb/schema";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { Environment } from "../../environment";
import type { EnvironmentStore } from "../../environment_store";
import { parsePackageName } from "../../identity";
import { packageTableSuffix } from "../../versions/materialization_scope";
import { MaterializationService } from "./materialization_service";
import { ownedTableComment, TABLE_OWNED_ELSEWHERE } from "./table_ownership";

/**
 * Two packages of one environment, on one shared warehouse connection, each
 * persisting a source of the same name and a source at the same `name=`, with
 * different rows. Real builds through the service, real queries: each
 * package must answer with its own rows, served from its own table or live,
 * never the other's. The environment case (two environments on one schema)
 * is the same rule, since the suffix hashes the environment too
 * (self_table_naming.spec.ts); it runs end to end on Postgres and DuckLake in
 * tests/integration/materialization/cross_package_tables.integration.spec.ts.
 */

const ENV = "e1";
const ENV_ID = "env-1";

/** The DuckDB `warehouse` connection both packages reach. */
const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

const suffix = (packageName: string) =>
   packageTableSuffix({ environmentName: ENV, packageName });

function model(who: string): string {
   return [
      "##! experimental.persistence",
      `source: base1 is warehouse.sql("SELECT '${who}' AS who, 1 AS k")`,
      `source: base2 is warehouse.sql("SELECT '${who}' AS who, 2 AS k")`,
      "#@ persist",
      "source: summary is base1 -> { group_by: who }",
      '#@ persist name="named_t"',
      "source: named is base2 -> { group_by: who }",
      "",
   ].join("\n");
}

describe("two packages on one warehouse connection", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restoreSettings: () => void;

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }).restore;
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "cross-package-tables-")),
      );
      envPath = path.join(root, "env");
      fs.mkdirSync(envPath);
      for (const pkg of ["pa", "pb"]) {
         writePackageFiles(
            path.join(envPath, pkg),
            { "model.malloy": model(pkg) },
            { manifest: { name: pkg, description: "fixture" } },
         );
      }
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         [ENV_ID, ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      await env.addPackage("pa");
      await env.addPackage("pb");
      const store = {
         storageManager: { getRepository: () => repo },
         getEnvironment: async () => env,
         getLoadedEnvironments: () => [env],
      } as unknown as EnvironmentStore;
      service = new MaterializationService(store);
   });

   afterEach(async () => {
      await env.closeAllConnections().catch(() => undefined);
      await db.close();
      fs.rmSync(root, { recursive: true, force: true });
      restoreSettings();
   });

   async function build(packageName: string): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, packageName, {});
      const deadline = Date.now() + 60_000;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (
            m &&
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
         ) {
            const running = (
               service as unknown as {
                  runningAbortControllers: Map<string, unknown>;
               }
            ).runningAbortControllers;
            while (running.has(created.id)) {
               await new Promise((resolve) => setTimeout(resolve, 10));
            }
            return (await repo.getMaterializationById(created.id)) ?? m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   }

   async function sql(text: string): Promise<Record<string, unknown>[]> {
      const connection = await env.getMalloyConnection("warehouse");
      return (await connection.runSQL(text)).rows as Record<string, unknown>[];
   }

   async function tables(): Promise<string[]> {
      return (
         await sql(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' ORDER BY 1",
         )
      ).map((r) => String(r.table_name));
   }

   async function comment(table: string): Promise<unknown> {
      return (
         await sql(
            `SELECT comment FROM duckdb_tables() WHERE table_name = '${table}'`,
         )
      )[0]?.comment;
   }

   /** What `source` answers in `packageName`. */
   async function who(packageName: string, source: string): Promise<string> {
      const pkg = await env.getPackage(parsePackageName(packageName), false);
      const result = await pkg
         .getModel("model.malloy")!
         .runQuery(unrecordedAudit(), {
            query: `run: ${source} -> { select: who }`,
         });
      const rows = (result as unknown as { compactResult: { who: string }[] })
         .compactResult;
      return rows[0].who;
   }

   /** Make a stored table answer `value`, so a reader of it is told apart. */
   const tamper = (table: string, value: string) =>
      sql(`UPDATE ${table} SET who = '${value}'`);

   /**
    * Why a run's sources failed: per source on a run that committed, or the
    * run's error when every source it built failed (a run whose only
    * rebuild was refused fails, and the package serves its run before).
    */
   const failureReasons = (m: Materialization) => [
      ...Object.values(
         (
            m as unknown as {
               manifest?: { failures?: Record<string, { reason: string }> };
            }
         ).manifest?.failures ?? {},
      ).map((f) => f.reason),
      ...(m.error ? [m.error] : []),
   ];

   it("each package answers with its own rows, from its own table or live", async () => {
      const a = await build("pa");
      const b = await build("pb");
      expect(a.status).toBe("MANIFEST_FILE_READY");
      expect(b.status).toBe("MANIFEST_FILE_READY");

      // Default names are the package's own; the author's name= is one table.
      expect(await tables()).toEqual(
         [`named_t`, `summary${suffix("pa")}`, `summary${suffix("pb")}`].sort(),
      );
      expect(await comment("named_t")).toBe(ownedTableComment(`${ENV}/pa`));
      // pb's name= is held by pa's table: refused, coded, nothing written.
      const reasons = failureReasons(b);
      expect(reasons).toHaveLength(1);
      expect(reasons[0]).toContain(TABLE_OWNED_ELSEWHERE);
      expect(reasons[0]).toContain(`a table package '${ENV}/pa' built`);

      // Served from the stored tables (tampered values prove it), or live.
      await tamper(`summary${suffix("pa")}`, "pa-stored");
      await tamper(`summary${suffix("pb")}`, "pb-stored");
      await tamper("named_t", "pa-stored-named");
      expect(await who("pa", "summary")).toBe("pa-stored");
      expect(await who("pb", "summary")).toBe("pb-stored");
      expect(await who("pa", "named")).toBe("pa-stored-named");
      // pb's `named` is answered live: its own rows, never pa's table.
      expect(await who("pb", "named")).toBe("pb");

      // A second build keeps it so: pa reuses its marked table, pb is
      // refused again and stays live.
      const b2 = await build("pb");
      expect(failureReasons(b2).join(" ")).toContain(TABLE_OWNED_ELSEWHERE);
      expect(await who("pb", "named")).toBe("pb");
      const a2 = await build("pa");
      expect(a2.status).toBe("MANIFEST_FILE_READY");
      expect(await who("pa", "named")).toBe("pa-stored-named");
   });

   it("deleting a run never drops a table another package has built at that name since", async () => {
      // pb builds `named_t` first; then the table is dropped outside
      // Publisher, and pa builds it.
      const b = await build("pb");
      expect(await comment("named_t")).toBe(ownedTableComment(`${ENV}/pb`));
      await sql("DROP TABLE named_t");
      const a = await build("pa");
      expect(a.status).toBe("MANIFEST_FILE_READY");
      expect(await comment("named_t")).toBe(ownedTableComment(`${ENV}/pa`));
      await tamper("named_t", "pa-stored-named");

      // pb's next build does not reuse the table its records name: it is
      // pa's now, so pb is refused and answers live.
      const b2 = await build("pb");
      expect(failureReasons(b2).join(" ")).toContain(TABLE_OWNED_ELSEWHERE);
      expect(await who("pb", "named")).toBe("pb");

      // Deleting pb's first run with its tables drops pb's own table and
      // leaves pa's.
      await service.deleteMaterialization(ENV, "pb", b.id, {
         dropTables: true,
      });
      expect(await tables()).toContain("named_t");
      expect(await who("pa", "named")).toBe("pa-stored-named");
   });

   it("an upgrade rebuilds a bare default name, and the first package to build again claims a shared name=", async () => {
      // As a server before package-owned names left it: both packages wrote
      // `summary` and `named_t`, unmarked, and both records name them. pb
      // wrote last, so both serve pb's rows.
      await sql("CREATE TABLE summary AS SELECT 'pb' AS who");
      await sql("CREATE TABLE named_t AS SELECT 'pb' AS who");
      for (const packageName of ["pa", "pb"]) {
         const plan = (
            await env.getPackage(parsePackageName(packageName), false)
         ).getBuildPlan()!;
         const entries = Object.fromEntries(
            Object.values(plan.sources).map((s) => [
               s.sourceEntityId,
               {
                  sourceEntityId: s.sourceEntityId,
                  sourceName: s.name,
                  physicalTableName: s.name === "named" ? "named_t" : s.name,
                  connectionName: "warehouse",
                  realization: "COPY",
               },
            ]),
         );
         const legacy = await repo.createMaterialization(
            ENV_ID,
            packageName,
            "PENDING",
            { mode: "auto" },
         );
         await repo.updateMaterialization(legacy.id, {
            status: "MANIFEST_FILE_READY",
            manifest: { builtAt: new Date().toISOString(), entries } as never,
            completedAt: new Date(),
         } as never);
      }

      const a = await build("pa");
      expect(a.status).toBe("MANIFEST_FILE_READY");
      const b = await build("pb");
      // The bare `summary` is never carried: each package rebuilt its own.
      expect(
         Object.values(a.manifest?.entries ?? {})
            .map((e) => e.physicalTableName)
            .sort(),
      ).toEqual(["named_t", `summary${suffix("pa")}`].sort());
      // pa built first: its records name the unmarked `named_t`, so it
      // claimed it. pb's records name it too, but pa's mark decides.
      expect(await comment("named_t")).toBe(ownedTableComment(`${ENV}/pa`));
      expect(failureReasons(b).join(" ")).toContain(TABLE_OWNED_ELSEWHERE);
      expect(await who("pa", "summary")).toBe("pa");
      expect(await who("pb", "summary")).toBe("pb");
      expect(await who("pa", "named")).toBe("pa");
      expect(await who("pb", "named")).toBe("pb");
      // Nothing was dropped: the bare table is still there, named only by
      // the legacy records.
      expect(await tables()).toContain("summary");
   });

   it("two packages building one name= at once: one builds it, the other is refused", async () => {
      for (let i = 0; i < 3; i++) {
         await sql("DROP TABLE IF EXISTS named_t");
         const [a, b] = await Promise.all([build("pa"), build("pb")]);
         const refused = [a, b].filter((m) =>
            failureReasons(m).join(" ").includes(TABLE_OWNED_ELSEWHERE),
         );
         expect(refused).toHaveLength(1);
         // The table holds the rows of the package its mark names.
         const holder = refused[0] === a ? "pb" : "pa";
         expect(await comment("named_t")).toBe(
            ownedTableComment(`${ENV}/${holder}`),
         );
         expect((await sql("SELECT who FROM named_t"))[0].who).toBe(holder);
         expect(await who("pa", "named")).toBe("pa");
         expect(await who("pb", "named")).toBe("pb");
      }
   });

   it("a name= spelled as another package's own table name is refused", async () => {
      fs.writeFileSync(
         path.join(envPath, "pa", "model.malloy"),
         [
            "##! experimental.persistence",
            `source: base1 is warehouse.sql("SELECT 'pa' AS who, 1 AS k")`,
            `#@ persist name="summary${suffix("pb")}"`,
            "source: summary is base1 -> { group_by: who }",
            "",
         ].join("\n"),
      );
      await env.getPackage(parsePackageName("pa"), true);
      // pa goes first, onto a free name: it would have built pb's table.
      const a = await build("pa");
      expect(failureReasons(a).join(" ")).toContain(TABLE_OWNED_ELSEWHERE);
      expect(await tables()).toEqual([]);
      await build("pb");
      await build("pa");
      expect(await who("pa", "summary")).toBe("pa");
      expect(await who("pb", "summary")).toBe("pb");
      expect(await tables()).toEqual([`named_t`, `summary${suffix("pb")}`]);
   });
});
