// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Lives under `tests/unit/` (not `src/`) on purpose: it builds a real
// EnvironmentStore over the real StorageManager, which sibling specs under
// `src/` replace with `mock.module(...)`. Bun's module mocks persist
// process-wide across spec files, so under `src/` it would get the mock; the
// integration runner gives it a clean module cache (see theme_store.spec.ts).

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { PackageController } from "../../../src/controller/package.controller";
import { VersionController } from "../../../src/controller/version.controller";
import type { Materialization } from "../../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../../src/storage/duckdb/DuckDBRepository";
import type { ResourceRepository } from "../../../src/storage/DatabaseInterface";
import { initializeSchema } from "../../../src/storage/duckdb/schema";
import { Environment } from "../../../src/service/environment";
import { EnvironmentStore } from "../../../src/service/environment_store";
import { logger } from "../../../src/logger";
import { MaterializationService } from "../../../src/service/materialization_service";
import {
   storageBindingResolverFor,
   versionTableSuffix,
   writeTargetsIn,
} from "../../../src/service/versions/materialization_scope";

// What archiving a version reclaims, and what it must leave alone: the keep
// rules of MaterializationService.reclaimVersionTables (a table another
// committed run still references; an orchestrated run's tables; anything
// under `scope: package`), the startup sweep that finishes a reclaim a crash
// cut short, and the archive hook and sweep as main_server.ts wires them onto a
// real EnvironmentStore. Builds are real, into a DuckDB file every version
// reaches (the environment's `warehouse` connection), so a dropped table is
// really gone and a kept one really answers.

const PKG = "sales";
const OTHER = "other";

type Scope = "version" | "package";

/** The table a `scope: version` version builds the source `base` into. */
const versionTable = (
   environmentName: string,
   version: string,
   packageName = PKG,
   base = "summary",
) => `${base}${versionTableSuffix({ environmentName, packageName, version })}`;

/**
 * A package whose persisted `summary` answers `answer`; with `detail`, a
 * second persisted source beside it. Different answers, different
 * definitions.
 */
function writePackage(
   sourcesPath: string,
   packageName: string,
   version: string,
   answer: number,
   scope: Scope,
   { detail = false }: { detail?: boolean } = {},
): string {
   const dir = path.join(
      sourcesPath,
      `${packageName}-${version}-${answer}-${scope}${detail ? "-detail" : ""}`,
   );
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({
         name: packageName,
         version,
         materialization: { scope },
      }),
   );
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      [
         "##! experimental.persistence",
         `source: base is warehouse.sql("SELECT ${answer} AS answer")`,
         '#@ persist name="summary"',
         "source: summary is base -> { group_by: answer }",
         ...(detail
            ? [
                 '#@ persist name="detail"',
                 "source: detail is base -> { group_by: doubled is answer * 2 }",
              ]
            : []),
         "",
      ].join("\n"),
   );
   return dir;
}

/** The DuckDB `warehouse` connection every version of the environment reaches. */
const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   // An environment DuckDB connection must carry setup SQL (or attached
   // databases); a no-op does.
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

/** Wait until a run has settled and the service has stopped tracking it. */
async function settledRun(
   repository: Pick<ResourceRepository, "getMaterializationById">,
   service: MaterializationService,
   id: string,
): Promise<Materialization> {
   const deadline = Date.now() + 60_000;
   for (;;) {
      const m = await repository.getMaterializationById(id);
      if (
         m &&
         (m.status === "MANIFEST_FILE_READY" ||
            m.status === "FAILED" ||
            m.status === "CANCELLED")
      ) {
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         while (running.has(id)) {
            if (Date.now() > deadline) throw new Error(`run ${id} never ended`);
            await new Promise((resolve) => setTimeout(resolve, 10));
         }
         return (await repository.getMaterializationById(id)) ?? m;
      }
      if (Date.now() > deadline) throw new Error(`run ${id} never settled`);
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
}

async function tablesIn(environment: Environment): Promise<string[]> {
   const connection = await environment.getMalloyConnection("warehouse");
   const result = await connection.runSQL(
      "SELECT table_name FROM information_schema.tables ORDER BY table_name",
   );
   return (result.rows as Record<string, unknown>[]).map((r) =>
      String(r.table_name),
   );
}

let setupSqlGate: string | undefined;

beforeEach(() => {
   setupSqlGate = process.env.PUBLISHER_ALLOW_DUCKDB_SETUP_SQL;
   process.env.PUBLISHER_ALLOW_DUCKDB_SETUP_SQL = "true";
});

afterEach(() => {
   if (setupSqlGate === undefined) {
      delete process.env.PUBLISHER_ALLOW_DUCKDB_SETUP_SQL;
   } else {
      process.env.PUBLISHER_ALLOW_DUCKDB_SETUP_SQL = setupSqlGate;
   }
});

// ---------------------------------------------------------------------------
// The service, over an Environment bound as EnvironmentStore binds one (the
// same setup as materialization_versions.spec.ts).
// ---------------------------------------------------------------------------

describe("reclaiming an archived version's tables", () => {
   const ENV = "testEnv";
   const ENV_ID = "env-1";
   const TABLE_V1 = versionTable(ENV, "1.0.0");
   const TABLE_V2 = versionTable(ENV, "2.0.0");
   const DETAIL_V1 = versionTable(ENV, "1.0.0", PKG, "detail");

   let root: string;
   let envPath: string;
   let sourcesPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   const environments: Environment[] = [];
   /** The reclaims archives started, so a test can wait for them. */
   let reclaims: Promise<void>[] = [];

   async function newEnvironment(): Promise<Environment> {
      const created = await Environment.create(ENV, envPath, [
         WAREHOUSE as never,
      ]);
      environments.push(created);
      created.bindVersions(repo, ENV_ID, {
         downloaderFor: (_packageName, location) => async (stagingPath) => {
            await fs.promises.cp(location, stagingPath, { recursive: true });
         },
         ensurePackageRecord: async (packageName, description) => {
            if (await repo.getPackageByName(ENV_ID, packageName)) return false;
            await repo.createPackage({
               environmentId: ENV_ID,
               name: packageName,
               description,
               manifestPath: "",
            });
            return true;
         },
         removePackageRecord: async (packageName) => {
            const row = await repo.getPackageByName(ENV_ID, packageName);
            if (row) await repo.deletePackage(row.id);
         },
      });
      // As EnvironmentStore.bindVersionRegistry sets it.
      created.setStorageBindingResolver(
         storageBindingResolverFor(repo, ENV_ID),
      );
      return created;
   }

   /** A MaterializationService over `environment`, its archives reclaiming. */
   function newService(environment: Environment): MaterializationService {
      const store = {
         storageManager: { getRepository: () => repo },
         getEnvironment: async () => environment,
         getLoadedEnvironments: () => [environment],
      } as unknown as EnvironmentStore;
      const created = new MaterializationService(store);
      environment.setVersionArchivedHook((packageName, versionId) => {
         reclaims.push(
            created.reclaimVersionTables(ENV, packageName, versionId),
         );
      });
      environment.setVersionDeletedHook((packageName, versionId) => {
         reclaims.push(
            created.cleanUpDeletedVersion(ENV, packageName, versionId),
         );
      });
      return created;
   }

   /** A restart: a fresh Environment and service over the same store. */
   async function restart(): Promise<void> {
      env = await newEnvironment();
      service = newService(env);
   }

   async function publish(
      version: string,
      answer: number,
      scope: Scope,
      { packageName = PKG, detail = false } = {},
   ) {
      const location = writePackage(
         sourcesPath,
         packageName,
         version,
         answer,
         scope,
         { detail },
      );
      return env.getVersionService()!.publish(
         packageName,
         async (stagingPath) => {
            await fs.promises.cp(location, stagingPath, { recursive: true });
         },
         { sourceLocation: location, promotion: "on-publish" },
      );
   }

   async function build(
      options: Parameters<
         MaterializationService["createMaterialization"]
      >[2] = {},
      packageName = PKG,
   ): Promise<Materialization> {
      const created = await service.createMaterialization(
         ENV,
         packageName,
         options,
      );
      return settledRun(repo, service, created.id);
   }

   /** Build `sourceName` of a version into `table`, as a host instructs it. */
   async function buildInstructed(
      versionId: string,
      sourceName: string,
      table: string,
   ): Promise<Materialization> {
      const plan = (
         await env.getPackage(PKG, false, { versionId })
      ).getBuildPlan()!;
      const source = Object.values(plan.sources).find(
         (s) => s.name === sourceName,
      );
      expect(source).toBeDefined();
      return build({
         versionId,
         buildInstructions: [
            {
               sourceID: source!.sourceID,
               sourceEntityId: source!.sourceEntityId,
               physicalTableName: table,
               realization: "COPY",
            },
         ] as never,
      });
   }

   async function archive(version: string, packageName = PKG) {
      await env
         .getVersionService()!
         .setArchiveStatus(packageName, version, "archive");
      await Promise.all(reclaims);
   }

   /** What a version's `summary` answers, and so which table it reads. */
   async function answerOf(versionId?: string): Promise<number> {
      const pkg = await env.getPackage(PKG, false, { versionId });
      const result = await pkg
         .getModel("model.malloy")!
         .getQueryResults(
            undefined,
            undefined,
            "run: summary -> { select: answer }",
         );
      const rows = (
         result as unknown as { compactResult?: { answer: number }[] }
      ).compactResult;
      return Number(rows![0].answer);
   }

   /** Make a built table answer `answer`, so whoever reads it is told apart. */
   async function tamper(table: string, answer: number): Promise<void> {
      const connection = await env.getMalloyConnection("warehouse");
      await connection.runSQL(`UPDATE ${table} SET answer = ${answer}`);
   }

   const statusOf = async (id: string) =>
      (await repo.getMaterializationById(id))?.status ?? null;

   beforeEach(async () => {
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "materialization-reclaim-")),
      );
      envPath = path.join(root, "env");
      sourcesPath = path.join(root, "sources");
      fs.mkdirSync(envPath);
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         [ENV_ID, ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      reclaims = [];
      env = await newEnvironment();
      service = newService(env);
   });

   afterEach(async () => {
      for (const environment of environments.splice(0)) {
         await environment.closeAllConnections().catch(() => undefined);
      }
      await db.close();
      fs.rmSync(root, { recursive: true, force: true });
   });

   it("drops a table only the archived version's runs reference, and keeps one another committed run still serves", async () => {
      // 1.0.0 builds `summary` and `detail` into tables of its own. A host
      // then builds 2.0.0's `summary` into 1.0.0's `summary` table, so a
      // committed run of another version references that one table.
      await publish("1.0.0", 1, "version", { detail: true });
      await publish("2.0.0", 2, "version", { detail: true });
      const v1 = await build({ versionId: "1.0.0" });
      expect(v1.status).toBe("MANIFEST_FILE_READY");
      const host = await buildInstructed("2.0.0", "summary", TABLE_V1);
      expect(host.status).toBe("MANIFEST_FILE_READY");
      expect(await tablesIn(env)).toEqual([DETAIL_V1, TABLE_V1]);

      // 2.0.0 serves `summary` from that table.
      await restart();
      await tamper(TABLE_V1, 202);
      expect(await answerOf("2.0.0")).toBe(202);

      await archive("1.0.0");

      // `detail`'s table was 1.0.0's alone: dropped. `summary`'s is still
      // referenced by 2.0.0's run: kept, and still served.
      expect(await tablesIn(env)).toEqual([TABLE_V1]);
      expect(await answerOf("2.0.0")).toBe(202);
      // The archived version's run is reclaimed; the other run is untouched.
      expect(await statusOf(v1.id)).toBeNull();
      expect(await statusOf(host.id)).toBe("MANIFEST_FILE_READY");
      // And 2.0.0 binds it again after a restart.
      await restart();
      expect(await answerOf("2.0.0")).toBe(202);
   });

   it("never drops the tables an orchestrated run of the archived version built, and keeps its record", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      const auto = await build({ versionId: "1.0.0" });
      const orchestrated = await buildInstructed("1.0.0", "summary", "host_v1");
      expect([auto.status, orchestrated.status]).toEqual([
         "MANIFEST_FILE_READY",
         "MANIFEST_FILE_READY",
      ]);
      // Both are this version's own runs: only `mode` tells them apart.
      expect(orchestrated.metadata).toMatchObject({
         mode: "orchestrated",
         scope: "version",
         versionId: "1.0.0",
      });
      expect((await tablesIn(env)).sort()).toEqual(
         ["host_v1", TABLE_V1].sort(),
      );

      await archive("1.0.0");

      // The auto-run's table and record are reclaimed; the host's are not.
      expect(await tablesIn(env)).toEqual(["host_v1"]);
      expect(await statusOf(auto.id)).toBeNull();
      const kept = await repo.getMaterializationById(orchestrated.id);
      expect(kept?.status).toBe("MANIFEST_FILE_READY");
      expect(
         Object.values(kept?.manifest?.entries ?? {}).map(
            (e) => e.physicalTableName,
         ),
      ).toEqual(["host_v1"]);
   });

   it("reclaims the tables of a run that failed after building them, though its manifest names none", async () => {
      await publish("1.0.0", 1, "version", { detail: true });
      await publish("2.0.0", 2, "version");
      // The run builds both tables, then cannot commit: it ends FAILED with
      // no manifest, the shape a cancelled or crashed run leaves too.
      const commit = spyOn(
         service as unknown as {
            commitManifest: (...args: unknown[]) => Promise<void>;
         },
         "commitManifest",
      ).mockImplementationOnce(async () => {
         throw new Error("the store refused the commit");
      });
      const failed = await build({ versionId: "1.0.0" });
      commit.mockRestore();
      expect(failed.status).toBe("FAILED");
      expect(Object.keys(failed.manifest?.entries ?? {})).toEqual([]);
      expect((await tablesIn(env)).sort()).toEqual(
         [DETAIL_V1, TABLE_V1].sort(),
      );
      // Before it built, it recorded where each table lives.
      expect(
         writeTargetsIn(failed.metadata ?? null)
            .map((t) => `${t.connectionName}:${t.physicalTableName}`)
            .sort(),
      ).toEqual([`warehouse:${DETAIL_V1}`, `warehouse:${TABLE_V1}`].sort());

      await archive("1.0.0");

      // Both tables are dropped and the record goes with them, so nothing
      // is left behind that no record names.
      expect(await tablesIn(env)).toEqual([]);
      expect(await statusOf(failed.id)).toBeNull();
   });

   it("reclaims nothing of a `scope: package` version, even when the package's later version is `scope: version`", async () => {
      // Whose a run's tables are is the run's own scope, not the package's
      // current one: 1.0.0 built the shared `summary`, 2.0.0 its own table.
      await publish("1.0.0", 1, "package");
      const shared = await build({});
      expect(shared.metadata).toMatchObject({
         versionId: "1.0.0",
         scope: "package",
      });
      await publish("2.0.0", 2, "version");
      const owned = await build({});
      expect(owned.metadata).toMatchObject({
         versionId: "2.0.0",
         scope: "version",
      });

      await archive("1.0.0");
      // Nor does the startup sweep.
      await service.reclaimArchivedVersions();

      expect(await tablesIn(env)).toEqual(["summary", TABLE_V2]);
      expect(await statusOf(shared.id)).toBe("MANIFEST_FILE_READY");
      expect(await statusOf(owned.id)).toBe("MANIFEST_FILE_READY");
      // Back in service, 1.0.0 still reads the shared table its run built.
      await env
         .getVersionService()!
         .setArchiveStatus(PKG, "1.0.0", "unarchive");
      await tamper("summary", 101);
      expect(await answerOf("1.0.0")).toBe(101);
   });

   it("the startup sweep reclaims an archived version whose reclaim never ran, in every package, and leaves versions in service alone", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      await publish("1.0.0", 7, "version", { packageName: OTHER });
      await publish("2.0.0", 8, "version", { packageName: OTHER });
      const v1 = await build({ versionId: "1.0.0" });
      const v2 = await build({ versionId: "2.0.0" });
      const other1 = await build({ versionId: "1.0.0" }, OTHER);
      const otherTable = versionTable(ENV, "1.0.0", OTHER);

      // The process stops between each archive's commit and its reclaim.
      env.setVersionArchivedHook(null);
      await archive("1.0.0");
      await archive("1.0.0", OTHER);
      expect(reclaims).toEqual([]);
      expect(await statusOf(v1.id)).toBe("MANIFEST_FILE_READY");
      expect(await statusOf(other1.id)).toBe("MANIFEST_FILE_READY");
      expect((await tablesIn(env)).sort()).toEqual(
         [otherTable, TABLE_V1, TABLE_V2].sort(),
      );

      await restart();
      await service.reclaimArchivedVersions();

      expect(await tablesIn(env)).toEqual([TABLE_V2]);
      expect(await statusOf(v1.id)).toBeNull();
      expect(await statusOf(other1.id)).toBeNull();
      // 2.0.0 is in service: its run and table stay, and it still reads them.
      expect(await statusOf(v2.id)).toBe("MANIFEST_FILE_READY");
      await tamper(TABLE_V2, 202);
      expect(await answerOf("2.0.0")).toBe(202);
   });

   it("a reclaim that starts after the version was unarchived again drops nothing", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      const v1 = await build({ versionId: "1.0.0" });

      // The archive's reclaim is started, but runs only after an unarchive.
      const started: [string, string][] = [];
      env.setVersionArchivedHook((packageName, versionId) => {
         started.push([packageName, versionId]);
      });
      const versions = env.getVersionService()!;
      await versions.setArchiveStatus(PKG, "1.0.0", "archive");
      await versions.setArchiveStatus(PKG, "1.0.0", "unarchive");
      expect(started).toEqual([[PKG, "1.0.0"]]);
      await service.reclaimVersionTables(ENV, PKG, "1.0.0");

      expect(await tablesIn(env)).toEqual([TABLE_V1]);
      expect(await statusOf(v1.id)).toBe("MANIFEST_FILE_READY");
      // Back in service, it still reads the table it built.
      await tamper(TABLE_V1, 101);
      expect(await answerOf("1.0.0")).toBe(101);
   });

   it("deleting a version drops its own tables once its row is gone, and keeps an orchestrated run's and other versions'", async () => {
      await publish("1.0.0", 1, "version", { detail: true });
      await publish("2.0.0", 2, "version");
      const v1 = await build({ versionId: "1.0.0" });
      const orchestrated = await buildInstructed("1.0.0", "summary", "host_v1");
      const v2 = await build({ versionId: "2.0.0" });
      expect((await tablesIn(env)).sort()).toEqual(
         [DETAIL_V1, "host_v1", TABLE_V1, TABLE_V2].sort(),
      );

      const deleted = await env
         .getVersionService()!
         .deleteVersion(PKG, "1.0.0");
      expect(deleted.packageRemoved).toBe(false);

      // The cleanup runs after the delete answers.
      await Promise.all(reclaims);
      expect((await tablesIn(env)).sort()).toEqual(
         ["host_v1", TABLE_V2].sort(),
      );
      expect(await statusOf(v1.id)).toBeNull();
      // The host's run is the host's: table and record stay.
      expect(await statusOf(orchestrated.id)).toBe("MANIFEST_FILE_READY");
      // 2.0.0 keeps its run and still reads its table.
      expect(await statusOf(v2.id)).toBe("MANIFEST_FILE_READY");
      await tamper(TABLE_V2, 202);
      expect(await answerOf("2.0.0")).toBe(202);
   });

   it("deleting a package's only version drops its tables and takes the package's records", async () => {
      await publish("1.0.0", 1, "version");
      const v1 = await build({ versionId: "1.0.0" });
      expect(await tablesIn(env)).toEqual([TABLE_V1]);

      const deleted = await env
         .getVersionService()!
         .deleteVersion(PKG, "1.0.0");
      expect(deleted.packageRemoved).toBe(true);
      expect(await repo.getPackageByName(ENV_ID, PKG)).toBeNull();
      // The package went first; its version's own run waited for the
      // cleanup, which drops the table and then the run.
      await Promise.all(reclaims);
      expect(await tablesIn(env)).toEqual([]);
      expect(await statusOf(v1.id)).toBeNull();
      expect(await repo.listMaterializations(ENV_ID, PKG)).toEqual([]);
   });

   it("the startup pass finishes a deleted version's cleanup that a crash cut short, its package gone with it", async () => {
      await publish("1.0.0", 1, "version");
      const v1 = await build({ versionId: "1.0.0" });
      // The server stops before the cleanup starts.
      env.setVersionDeletedHook(null);
      await env.getVersionService()!.deleteVersion(PKG, "1.0.0");
      expect(await repo.getPackageByName(ENV_ID, PKG)).toBeNull();
      expect(await tablesIn(env)).toEqual([TABLE_V1]);
      expect(await statusOf(v1.id)).toBe("MANIFEST_FILE_READY");

      await restart();
      await service.reclaimArchivedVersions();
      expect(await tablesIn(env)).toEqual([]);
      expect(await statusOf(v1.id)).toBeNull();
   });

   it("a delete's cleanup stops the deleted version's runs that have not settled, and leaves other versions' runs", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      // A run of 1.0.0 that is running here (it is aborted), one that has not
      // started (it is marked cancelled), and a run of 2.0.0.
      const running = await repo.createMaterialization(
         ENV_ID,
         PKG,
         "PENDING",
         { scope: "version", mode: "auto" },
         { version: "1.0.0", lockVersion: "1.0.0" },
      );
      // Each in a slot of its own: one active run per slot.
      const waiting = await repo.createMaterialization(
         ENV_ID,
         PKG,
         "PENDING",
         { scope: "version", mode: "orchestrated" },
         { version: "1.0.0" },
      );
      const other = await repo.createMaterialization(
         ENV_ID,
         PKG,
         "PENDING",
         { scope: "version", mode: "auto" },
         { version: "2.0.0", lockVersion: "2.0.0" },
      );
      const abort = new AbortController();
      (
         service as unknown as {
            runningAbortControllers: Map<string, AbortController>;
         }
      ).runningAbortControllers.set(running.id, abort);

      await env.getVersionService()!.deleteVersion(PKG, "1.0.0");
      await Promise.all(reclaims);

      expect(abort.signal.aborted).toBe(true);
      expect(await statusOf(waiting.id)).toBe("CANCELLED");
      expect(await statusOf(other.id)).toBe("PENDING");
   });

   it("tries a table it could not drop once: it is named in a warning and left, and the startup pass tries nothing", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      const v1 = await build({ versionId: "1.0.0" });
      const v2 = await build({ versionId: "2.0.0" });

      // The delete's drop fails: every table is left.
      const drop = spyOn(
         service as unknown as {
            dropManifestEntries: (...args: unknown[]) => Promise<unknown>;
         },
         "dropManifestEntries",
      ).mockImplementationOnce(async (...args: unknown[]) => args[4]);
      const warn = spyOn(logger, "warn");
      try {
         await env.getVersionService()!.deleteVersion(PKG, "1.0.0");
         await Promise.all(reclaims);
         const left = warn.mock.calls.filter(([message]) =>
            String(message).includes("could not drop"),
         );
         expect(left).toHaveLength(1);
         expect(JSON.stringify(left[0][1])).toContain(TABLE_V1);
      } finally {
         drop.mockRestore();
         warn.mockRestore();
      }
      expect(await repo.getVersion(ENV_ID, PKG, "1.0.0")).toBeNull();
      // Its record goes all the same, so nothing comes back to it.
      expect(await statusOf(v1.id)).toBeNull();
      expect((await tablesIn(env)).sort()).toEqual([TABLE_V1, TABLE_V2].sort());

      await restart();
      await service.reclaimArchivedVersions();

      expect((await tablesIn(env)).sort()).toEqual([TABLE_V1, TABLE_V2].sort());
      expect(await statusOf(v2.id)).toBe("MANIFEST_FILE_READY");
   });
});

// ---------------------------------------------------------------------------
// The production wiring: a real EnvironmentStore, its archive hook and its
// startup sweep registered exactly as main_server.ts registers them.
// ---------------------------------------------------------------------------

describe("the archive hook and the startup sweep, as main_server.ts wires them", () => {
   const ENV = "wired";
   const TABLE_V1 = versionTable(ENV, "1.0.0");
   const TABLE_V2 = versionTable(ENV, "2.0.0");

   let root: string;
   let serverRoot: string;
   let sourcesPath: string;
   let versioningGate: string | undefined;
   let promotionGate: string | undefined;
   const opened: EnvironmentStore[] = [];

   interface Server {
      store: EnvironmentStore;
      service: MaterializationService;
      /** Every reclaimVersionTables call, and the reclaims it started. */
      calls: string[][];
      reclaims: Promise<void>[];
      /** The startup sweep, when wired. */
      sweep: Promise<void>;
   }

   /**
    * A server over `serverRoot`. With `wired`, its archive hook and startup
    * sweep are registered as main_server.ts registers them (the closures below
    * are main_server.ts's, verbatim); without, it is a process that stops before
    * any reclaim runs. reclaimVersionTables is observed, never replaced.
    */
   async function openServer({ wired = true } = {}): Promise<Server> {
      const store = new EnvironmentStore(serverRoot);
      opened.push(store);
      const materializationService = new MaterializationService(store);
      const calls: string[][] = [];
      const reclaims: Promise<void>[] = [];
      const reclaim = materializationService.reclaimVersionTables.bind(
         materializationService,
      );
      spyOn(materializationService, "reclaimVersionTables").mockImplementation(
         (
            ...args: Parameters<MaterializationService["reclaimVersionTables"]>
         ) => {
            calls.push([...args]);
            const started = reclaim(...args);
            reclaims.push(started);
            return started;
         },
      );
      const environmentStore = store;
      let sweep: Promise<void> = Promise.resolve();
      if (wired) {
         // Archiving a version reclaims the tables it alone owns, in the background.
         environmentStore.setVersionArchivedHook(
            (environmentName, packageName, versionId) => {
               void materializationService.reclaimVersionTables(
                  environmentName,
                  packageName,
                  versionId,
               );
            },
         );
         // Deleting a version stops its builds and reclaims those tables, in the
         // background, once its row is gone.
         environmentStore.setVersionDeletedHook(
            (environmentName, packageName, versionId) => {
               void materializationService.cleanUpDeletedVersion(
                  environmentName,
                  packageName,
                  versionId,
               );
            },
         );
         // A reclaim that a crash cut short is finished once the environments have
         // loaded: nothing else would start it again.
         sweep = environmentStore.finishedInitialization.then(
            () => materializationService.reclaimArchivedVersions(),
            () => undefined,
         );
      }
      await store.finishedInitialization;
      return {
         store,
         service: materializationService,
         calls,
         reclaims,
         sweep,
      };
   }

   async function closeServer(server: Server): Promise<void> {
      for (const environment of server.store.getLoadedEnvironments()) {
         await environment.closeAllConnections().catch(() => undefined);
      }
      await server.store.storageManager.close();
      opened.splice(opened.indexOf(server.store), 1);
   }

   async function publish(server: Server, version: string, answer: number) {
      const location = writePackage(
         sourcesPath,
         PKG,
         version,
         answer,
         "version",
      );
      await new PackageController(server.store).addPackage(ENV, {
         name: PKG,
         location,
      } as never);
   }

   async function build(
      server: Server,
      versionId: string,
   ): Promise<Materialization> {
      const created = await server.service.createMaterialization(ENV, PKG, {
         versionId,
      });
      return settledRun(
         server.store.storageManager.getRepository(),
         server.service,
         created.id,
      );
   }

   async function archive(server: Server, version: string): Promise<void> {
      const answered = await new VersionController(server.store).updateVersion(
         ENV,
         PKG,
         version,
         { archiveStatus: "archive" },
      );
      expect(answered.archiveStatus).toBe("archive");
   }

   async function tables(server: Server): Promise<string[]> {
      return tablesIn(await server.store.getEnvironment(ENV, false));
   }

   const statusOf = async (server: Server, id: string) =>
      (
         await server.store.storageManager
            .getRepository()
            .getMaterializationById(id)
      )?.status ?? null;

   beforeEach(() => {
      versioningGate = process.env.PUBLISHER_PACKAGE_VERSIONING;
      promotionGate = process.env.PUBLISHER_VERSION_PROMOTION;
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      delete process.env.PUBLISHER_VERSION_PROMOTION;
      root = fs.realpathSync(
         fs.mkdtempSync(
            path.join(os.tmpdir(), "materialization-reclaim-wired-"),
         ),
      );
      serverRoot = path.join(root, "server");
      sourcesPath = path.join(root, "sources");
      fs.mkdirSync(serverRoot);
   });

   afterEach(async () => {
      for (const store of opened.splice(0)) {
         for (const environment of store.getLoadedEnvironments()) {
            await environment.closeAllConnections().catch(() => undefined);
         }
         await store.storageManager.close().catch(() => undefined);
      }
      fs.rmSync(root, { recursive: true, force: true });
      for (const [name, value] of [
         ["PUBLISHER_PACKAGE_VERSIONING", versioningGate],
         ["PUBLISHER_VERSION_PROMOTION", promotionGate],
      ] as const) {
         if (value === undefined) delete process.env[name];
         else process.env[name] = value;
      }
   });

   it("archiving a version of an environment created at runtime reclaims its auto-run tables", async () => {
      const server = await openServer();
      await server.sweep;
      await server.store.addEnvironment({
         name: ENV,
         connections: [WAREHOUSE],
      } as never);
      await publish(server, "1.0.0", 1);
      await publish(server, "2.0.0", 2);
      const v1 = await build(server, "1.0.0");
      const v2 = await build(server, "2.0.0");
      expect(await tables(server)).toEqual([TABLE_V1, TABLE_V2]);

      await archive(server, "1.0.0");
      await Promise.all(server.reclaims);

      // Started by the archive, for this environment, package and version.
      expect(server.calls).toEqual([[ENV, PKG, "1.0.0"]]);
      expect(await tables(server)).toEqual([TABLE_V2]);
      expect(await statusOf(server, v1.id)).toBeNull();
      expect(await statusOf(server, v2.id)).toBe("MANIFEST_FILE_READY");
   });

   it("deleting a version through the controller drops its own tables and frees its id", async () => {
      const server = await openServer();
      await server.sweep;
      await server.store.addEnvironment({
         name: ENV,
         connections: [WAREHOUSE],
      } as never);
      await publish(server, "1.0.0", 1);
      await publish(server, "2.0.0", 2);
      const v1 = await build(server, "1.0.0");
      const v2 = await build(server, "2.0.0");
      expect(await tables(server)).toEqual([TABLE_V1, TABLE_V2]);

      const answered = await new VersionController(server.store).deleteVersion(
         ENV,
         PKG,
         "1.0.0",
      );
      expect(answered.id).toBe("1.0.0");
      // The cleanup runs after the delete answers: it stops the version's
      // builds, then reclaims.
      const deadline = Date.now() + 30_000;
      while (server.calls.length === 0 && Date.now() < deadline) {
         await new Promise((resolve) => setTimeout(resolve, 10));
      }
      await Promise.all(server.reclaims);
      expect(server.calls).toEqual([[ENV, PKG, "1.0.0"]]);
      expect(await tables(server)).toEqual([TABLE_V2]);
      expect(await statusOf(server, v1.id)).toBeNull();
      expect(await statusOf(server, v2.id)).toBe("MANIFEST_FILE_READY");
      // The id is free again: other content publishes under it.
      await publish(server, "1.0.0", 11);
      const again = await new VersionController(server.store).getVersion(
         ENV,
         PKG,
         "1.0.0",
      );
      expect(again.archiveStatus).toBe("unarchive");
   });

   // Skipped on Windows, where DuckDB keeps the OS handle on a file database
   // after close(), so the restarted store, in this same process, cannot open
   // the warehouse file again ("being used by another process"); the same
   // limitation as materialization_build_session.spec.ts. POSIX releases it.
   // The reclaim itself runs on Windows in the test above.
   it.skipIf(process.platform === "win32")(
      "after a restart, the startup sweep finishes a reclaim that never ran, and an environment reloaded from the database reclaims on archive",
      async () => {
         // A process that stopped between the archive's commit and its reclaim.
         const before = await openServer({ wired: false });
         await before.store.addEnvironment({
            name: ENV,
            connections: [WAREHOUSE],
         } as never);
         await publish(before, "1.0.0", 1);
         await publish(before, "2.0.0", 2);
         await publish(before, "3.0.0", 3);
         const v1 = await build(before, "1.0.0");
         const v2 = await build(before, "2.0.0");
         await archive(before, "1.0.0");
         expect(before.calls).toEqual([]);
         expect(await tables(before)).toEqual([TABLE_V1, TABLE_V2]);
         expect(await statusOf(before, v1.id)).toBe("MANIFEST_FILE_READY");
         await closeServer(before);

         const after = await openServer();
         await after.sweep;
         await Promise.all(after.reclaims);
         expect(after.calls).toEqual([[ENV, PKG, "1.0.0"]]);
         expect(await tables(after)).toEqual([TABLE_V2]);
         expect(await statusOf(after, v1.id)).toBeNull();
         expect(await statusOf(after, v2.id)).toBe("MANIFEST_FILE_READY");

         // The environment came back from the database with the hook attached.
         await archive(after, "2.0.0");
         await Promise.all(after.reclaims);
         expect(after.calls).toEqual([
            [ENV, PKG, "1.0.0"],
            [ENV, PKG, "2.0.0"],
         ]);
         expect(await tables(after)).toEqual([]);
         expect(await statusOf(after, v2.id)).toBeNull();
      },
   );

   it("main_server.ts registers that hook and that sweep on the materialization service", () => {
      // The two tests above register main_server.ts's closures on a real store;
      // this pins that main_server.ts registers them, which its module body does
      // at boot and no test can import without starting the server.
      const squash = (text: string) =>
         text
            .replace(/\r\n/g, "\n")
            .replace(/^\s*\/\/.*$/gm, "")
            .replace(/\s+/g, "")
            .replace(/,(?=[)}])/g, "");
      const server = squash(
         fs.readFileSync(
            path.resolve(import.meta.dir, "../../../src/main_server.ts"),
            "utf8",
         ),
      );
      expect(server).toContain(
         squash(
            "const materializationService = new MaterializationService(environmentStore);",
         ),
      );
      expect(server).toContain(
         squash(`environmentStore.setVersionArchivedHook(
            (environmentName, packageName, versionId) => {
               void materializationService.reclaimVersionTables(
                  environmentName,
                  packageName,
                  versionId,
               );
            },
         );`),
      );
      expect(server).toContain(
         squash(`environmentStore.setVersionDeletedHook(
            (environmentName, packageName, versionId) => {
               void materializationService.cleanUpDeletedVersion(
                  environmentName,
                  packageName,
                  versionId,
               );
            },
         );`),
      );
      expect(server).toContain(
         squash(`environmentStore.finishedInitialization.then(
            () => materializationService.reclaimArchivedVersions(),
            () => undefined,
         );`),
      );
   });
});
