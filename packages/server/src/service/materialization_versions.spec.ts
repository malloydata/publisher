// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   internalErrorToHttpError,
   MaterializationConflictError,
   PackageVersionError,
} from "../errors";
import { MaterializationController } from "../controller/materialization.controller";
import type { Materialization } from "../storage/DatabaseInterface";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { DuckDBRepository } from "../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../storage/duckdb/schema";
import { Environment } from "./environment";
import type { EnvironmentStore } from "./environment_store";
import { MaterializationScheduler } from "./materialization_scheduler";
import { MaterializationService } from "./materialization_service";
import { Package } from "./package";
import {
   SUPERSEDED_TABLES_KEY,
   ServingReader,
   storageBindingResolverFor,
} from "./versions/materialization_scope";

// Materializations of published versions, end to end: real versions compiled
// from real folders, real builds into a DuckDB file every version reaches (the
// environment's `warehouse` connection, so `scope: package` tables really are
// shared), and real queries. A test tampers with a built table so that a
// version reading it answers differently from one serving live: what a query
// answers says which table, if any, that version is bound to.

const ENV = "testEnv";
const ENV_ID = "env-1";
const PKG = "sales";

let root: string;
let envPath: string;
let sourcesPath: string;
let db: DuckDBConnection;
let repo: DuckDBRepository;
let env: Environment;
let service: MaterializationService;
const environments: Environment[] = [];

/**
 * A package whose persisted `summary` answers `answer`. Two versions with the
 * same answer have the same definition (one content address); different
 * answers, different definitions.
 */
function writePackage(
   version: string,
   answer: number,
   scope: "version" | "package",
   schedule?: string,
): string {
   const dir = path.join(
      sourcesPath,
      `${version}-${answer}-${scope}${schedule ? "-scheduled" : ""}`,
   );
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({
         name: PKG,
         version,
         materialization: { scope, ...(schedule ? { schedule } : {}) },
      }),
   );
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      [
         "##! experimental.persistence",
         `source: base is warehouse.sql("SELECT ${answer} AS answer")`,
         '#@ persist name="summary"',
         "source: summary is base -> { group_by: answer }",
         "",
      ].join("\n"),
   );
   return dir;
}

async function newEnvironment(): Promise<Environment> {
   const created = await Environment.create(ENV, envPath, [
      // A DuckDB file in the environment: every version reaches the same one.
      // An environment DuckDB connection must carry setup SQL (or attached
      // databases); a no-op does.
      {
         name: "warehouse",
         type: "duckdb",
         duckdbConnection: { setupSQL: "SELECT 1" },
      },
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
   created.setStorageBindingResolver(storageBindingResolverFor(repo, ENV_ID));
   return created;
}

/** A MaterializationService over `environment`, as server.ts wires it. */
function newService(environment: Environment): MaterializationService {
   const store = {
      storageManager: { getRepository: () => repo },
      getEnvironment: async () => environment,
      getLoadedEnvironments: () => [environment],
   } as unknown as EnvironmentStore;
   const created = new MaterializationService(store);
   environment.setVersionArchivedHook((packageName, versionId) => {
      reclaims.push(created.reclaimVersionTables(ENV, packageName, versionId));
   });
   return created;
}

/** The reclaims archives started, so a test can wait for them. */
let reclaims: Promise<void>[] = [];

async function publish(
   version: string,
   answer: number,
   scope: "version" | "package",
   promotion: "on-publish" | "explicit" = "on-publish",
   schedule?: string,
) {
   const location = writePackage(version, answer, scope, schedule);
   return env.getVersionService()!.publish(
      PKG,
      async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      { sourceLocation: location, promotion },
   );
}

/** Run a materialization to its end and return its final record. */
async function build(
   options: Parameters<MaterializationService["createMaterialization"]>[2] = {},
): Promise<Materialization> {
   const created = await service.createMaterialization(ENV, PKG, options);
   return settled(created.id);
}

async function settled(id: string): Promise<Materialization> {
   const deadline = Date.now() + 60_000;
   for (;;) {
      const m = await repo.getMaterializationById(id);
      if (
         m &&
         (m.status === "MANIFEST_FILE_READY" ||
            m.status === "FAILED" ||
            m.status === "CANCELLED")
      ) {
         // The run's settle hook (its version's build registration) runs
         // after its final status is written.
         await new Promise((resolve) => setTimeout(resolve, 10));
         return m;
      }
      if (Date.now() > deadline) throw new Error(`run ${id} never settled`);
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
}

/** What a version's `summary` answers, and so which table it reads. */
async function answerOf(versionId?: string): Promise<number> {
   const pkg = await env.getPackage(PKG, false, { versionId });
   const model = pkg.getModel("model.malloy");
   expect(model).toBeDefined();
   const result = await model!.getQueryResults(
      undefined,
      undefined,
      "run: summary -> { select: answer }",
   );
   const rows = (result as unknown as { compactResult?: { answer: number }[] })
      .compactResult;
   return Number(rows![0].answer);
}

async function warehouseSQL(sql: string): Promise<Record<string, unknown>[]> {
   const connection = await env.getMalloyConnection("warehouse");
   const result = await connection.runSQL(sql);
   return result.rows as Record<string, unknown>[];
}

async function warehouseTables(): Promise<string[]> {
   const rows = await warehouseSQL(
      "SELECT table_name FROM information_schema.tables ORDER BY table_name",
   );
   return rows.map((r) => String(r.table_name));
}

/** Make a built table answer `answer`, so whoever reads it is told apart. */
async function tamper(table: string, answer: number): Promise<void> {
   await warehouseSQL(`UPDATE ${table} SET answer = ${answer}`);
}

async function refusal(promise: Promise<unknown>) {
   const error = await promise.then(
      () => undefined,
      (err: unknown) => err,
   );
   expect(error).toBeInstanceOf(Error);
   const { status, json } = internalErrorToHttpError(error as Error, {
      log: false,
   });
   return { status, reason: (json as { reason?: string }).reason };
}

/** Hold every run at its first write until the returned release is called. */
function holdRuns(): () => void {
   let release!: () => void;
   const gate = new Promise<void>((resolve) => (release = resolve));
   const update = repo.updateMaterialization.bind(repo);
   const spy = spyOn(repo, "updateMaterialization").mockImplementation(
      async (...args: Parameters<typeof repo.updateMaterialization>) => {
         if (args[1].startedAt !== undefined) await gate;
         return update(...args);
      },
   );
   return () => {
      spy.mockRestore();
      release();
   };
}

let setupSqlGate: string | undefined;

beforeEach(async () => {
   setupSqlGate = process.env.PUBLISHER_ALLOW_DUCKDB_SETUP_SQL;
   process.env.PUBLISHER_ALLOW_DUCKDB_SETUP_SQL = "true";
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "materialization-versions-")),
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
   repo = new DuckDBRepository(db);
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
   if (setupSqlGate === undefined) {
      delete process.env.PUBLISHER_ALLOW_DUCKDB_SETUP_SQL;
   } else {
      process.env.PUBLISHER_ALLOW_DUCKDB_SETUP_SQL = setupSqlGate;
   }
});

describe('scope "version": each version builds and serves its own tables', () => {
   it("builds a version into tables named for it, and records the version on the run", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");

      const v1 = await build({ versionId: "1.0.0" });
      const v2 = await build({});

      expect([v1.status, v2.status]).toEqual([
         "MANIFEST_FILE_READY",
         "MANIFEST_FILE_READY",
      ]);
      expect([v1.version, v2.version]).toEqual(["1.0.0", "2.0.0"]);
      expect(v1.metadata).toMatchObject({
         versionId: "1.0.0",
         scope: "version",
      });
      expect(v2.metadata).toMatchObject({
         versionId: "2.0.0",
         scope: "version",
      });
      expect(
         Object.values(v1.manifest?.entries ?? {}).map(
            (e) => e.physicalTableName,
         ),
      ).toEqual(["summary__v1_0_0"]);
      expect(await warehouseTables()).toEqual([
         "summary__v1_0_0",
         "summary__v2_0_0",
      ]);
   });

   it("serves each version from its own table", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      await build({ versionId: "1.0.0" });
      await build({ versionId: "2.0.0" });
      await tamper("summary__v1_0_0", 101);
      await tamper("summary__v2_0_0", 102);

      expect(await answerOf("1.0.0")).toBe(101);
      expect(await answerOf("2.0.0")).toBe(102);
   });

   it("binds each version to its own table again after a restart", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      await build({ versionId: "1.0.0" });
      await build({ versionId: "2.0.0" });
      await tamper("summary__v1_0_0", 101);
      await tamper("summary__v2_0_0", 102);

      env = await newEnvironment();
      service = newService(env);
      expect(await answerOf("2.0.0")).toBe(102);
      expect(await answerOf("1.0.0")).toBe(101);
   });

   it("reuses only the version's own runs", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 1, "version"); // the same definition
      await build({ versionId: "1.0.0" });

      // Another version's run never counts as this version's reuse.
      const first = await build({ versionId: "2.0.0" });
      expect(first.metadata).toMatchObject({
         sourcesBuilt: 1,
         sourcesReused: 0,
      });
      const again = await build({ versionId: "2.0.0" });
      expect(again.metadata).toMatchObject({
         sourcesBuilt: 0,
         sourcesReused: 1,
      });
   });

   it("builds two versions at once, but one version only once at a time", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      const release = holdRuns();
      try {
         const a = await service.createMaterialization(ENV, PKG, {
            versionId: "1.0.0",
         });
         const b = await service.createMaterialization(ENV, PKG, {});
         expect([a.version, b.version]).toEqual(["1.0.0", "2.0.0"]);
         const again = await service
            .createMaterialization(ENV, PKG, { versionId: "1.0.0" })
            .catch((err: unknown) => err);
         expect(again).toBeInstanceOf(MaterializationConflictError);
         expect((again as Error).message).toContain("building version 1.0.0");
         release();
         expect((await settled(a.id)).status).toBe("MANIFEST_FILE_READY");
         expect((await settled(b.id)).status).toBe("MANIFEST_FILE_READY");
      } finally {
         release();
      }
   });

   it("archiving a version reclaims its own tables and runs, and nothing else", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      const v1 = await build({ versionId: "1.0.0" });
      await build({ versionId: "2.0.0" });

      await env.getVersionService()!.setArchiveStatus(PKG, "1.0.0", "archive");
      await Promise.all(reclaims);

      expect(await warehouseTables()).toEqual(["summary__v2_0_0"]);
      expect(await repo.getMaterializationById(v1.id)).toBeNull();
      expect(
         (await repo.listMaterializations(ENV_ID, PKG)).map((m) => m.version),
      ).toEqual(["2.0.0"]);
   });

   it("an unarchived version serves live until it is built again", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      await build({ versionId: "1.0.0" });
      await tamper("summary__v1_0_0", 101);
      expect(await answerOf("1.0.0")).toBe(101);

      const versions = env.getVersionService()!;
      await versions.setArchiveStatus(PKG, "1.0.0", "archive");
      await Promise.all(reclaims);
      await versions.setArchiveStatus(PKG, "1.0.0", "unarchive");

      expect(await answerOf("1.0.0")).toBe(1);
      await build({ versionId: "1.0.0" });
      await tamper("summary__v1_0_0", 111);
      expect(await answerOf("1.0.0")).toBe(111);
   });

   it("never archives a version while it builds, and never builds an archived one", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      const versions = env.getVersionService()!;
      const release = holdRuns();
      let run: Materialization;
      try {
         run = await service.createMaterialization(ENV, PKG, {
            versionId: "1.0.0",
         });
         expect(
            await refusal(versions.setArchiveStatus(PKG, "1.0.0", "archive")),
         ).toEqual({ status: 409, reason: "VERSION_BUILDING" });
      } finally {
         release();
      }
      expect((await settled(run.id)).status).toBe("MANIFEST_FILE_READY");

      // Settled: the archive goes through, and a run of it is refused.
      await versions.setArchiveStatus(PKG, "1.0.0", "archive");
      expect(
         await refusal(
            service.createMaterialization(ENV, PKG, { versionId: "1.0.0" }),
         ),
      ).toEqual({ status: 410, reason: "VERSION_ARCHIVED" });
   });

   it("an archive that lands while a run waits for the version's lock wins: the run is refused", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      const versions = env.getVersionService()!;
      // Hold the archive's write, which it makes holding the version's lock;
      // the run queues behind that lock.
      let unlock!: () => void;
      const gate = new Promise<void>((resolve) => (unlock = resolve));
      let reached!: () => void;
      const writing = new Promise<void>((resolve) => (reached = resolve));
      const write = repo.setVersionArchiveStatus.bind(repo);
      const spy = spyOn(repo, "setVersionArchiveStatus").mockImplementation(
         async (...args: Parameters<typeof repo.setVersionArchiveStatus>) => {
            reached();
            await gate;
            return write(...args);
         },
      );
      const archiving = versions.setArchiveStatus(PKG, "1.0.0", "archive");
      await writing;
      const creating = service.createMaterialization(ENV, PKG, {
         versionId: "1.0.0",
      });
      await new Promise((resolve) => setTimeout(resolve, 20));
      unlock();
      await archiving;
      spy.mockRestore();
      expect(await refusal(creating)).toEqual({
         status: 410,
         reason: "VERSION_ARCHIVED",
      });
      expect(await repo.listMaterializations(ENV_ID, PKG)).toEqual([]);
   });

   it("keeps the record of a table it could not drop, and retries it at the next sweep", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      const v1 = await build({ versionId: "1.0.0" });
      const connection = await env.getMalloyConnection("warehouse");
      const runSQL = connection.runSQL.bind(connection);
      const failing = spyOn(connection, "runSQL").mockImplementation(
         async (sql: string, ...rest: unknown[]) => {
            if (sql.startsWith("DROP TABLE")) throw new Error("warehouse down");
            return runSQL(sql, ...(rest as []));
         },
      );
      await env.getVersionService()!.setArchiveStatus(PKG, "1.0.0", "archive");
      await Promise.all(reclaims);
      failing.mockRestore();

      const kept = await repo.getMaterializationById(v1.id);
      expect(kept?.status).toBe("FAILED");
      expect(await warehouseTables()).toContain("summary__v1_0_0");

      await service.reclaimArchivedVersions();
      expect(await repo.getMaterializationById(v1.id)).toBeNull();
      expect(await warehouseTables()).not.toContain("summary__v1_0_0");
   });
});

describe('scope "package": the versions share the package\'s tables', () => {
   it("refuses an auto-run of a version that is not latest, and builds latest's into the shared table", async () => {
      await publish("1.0.0", 1, "package");
      await publish("2.0.0", 2, "package");

      expect(
         await refusal(
            service.createMaterialization(ENV, PKG, { versionId: "1.0.0" }),
         ),
      ).toEqual({ status: 400, reason: "VERSION_NOT_LATEST" });
      expect(await repo.listMaterializations(ENV_ID, PKG)).toEqual([]);

      const latest = await build({});
      expect(latest.version).toBe("2.0.0");
      expect(latest.metadata).toMatchObject({
         versionId: "2.0.0",
         scope: "package",
      });
      expect(await warehouseTables()).toEqual(["summary"]);
   });

   it("an older version never serves rows latest's definition built", async () => {
      await publish("1.0.0", 1, "package");
      await build({});
      await tamper("summary", 101);
      expect(await answerOf("1.0.0")).toBe(101);

      // Latest moves to a changed definition, and rebuilds the shared table.
      await publish("2.0.0", 2, "package");
      expect(await answerOf("1.0.0")).toBe(101);
      await build({});
      await tamper("summary", 202);

      expect(await answerOf("2.0.0")).toBe(202);
      // 1.0.0's definition did not build what is in `summary` now: live.
      expect(await answerOf("1.0.0")).toBe(1);

      // A restart binds the same way.
      env = await newEnvironment();
      service = newService(env);
      expect(await answerOf("1.0.0")).toBe(1);
      expect(await answerOf("2.0.0")).toBe(202);
   });

   it("while latest rebuilds the shared tables, every other version serves them live", async () => {
      await publish("1.0.0", 1, "package");
      await build({});
      await tamper("summary", 101);
      await publish("2.0.0", 2, "package");
      expect(await answerOf("1.0.0")).toBe(101);

      // Hold latest's run once it has said what it rebuilds, before it builds.
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      let reached!: () => void;
      const building = new Promise<void>((resolve) => (reached = resolve));
      const execute = (
         service as unknown as {
            executeInstructedBuild: (...args: unknown[]) => Promise<unknown>;
         }
      ).executeInstructedBuild.bind(service);
      const spy = spyOn(
         service as unknown as {
            executeInstructedBuild: (...args: unknown[]) => Promise<unknown>;
         },
         "executeInstructedBuild",
      ).mockImplementation(async (...args: unknown[]) => {
         reached();
         await gate;
         return execute(...args);
      });
      try {
         const run = await service.createMaterialization(ENV, PKG, {});
         await building;
         // `summary` is being rebuilt from 2.0.0's definition: not read.
         expect(await answerOf("1.0.0")).toBe(1);
         release();
         expect((await settled(run.id)).status).toBe("MANIFEST_FILE_READY");
      } finally {
         release();
         spy.mockRestore();
      }
   });

   it("a version that read the store before a run started, and loaded after it, is settled once it is findable", async () => {
      await publish("1.0.0", 1, "package");
      await build({});
      await tamper("summary", 101);
      await publish("2.0.0", 2, "package");

      // A restart, and a load of 1.0.0 that reads the store (binding the
      // table 1.0.0 built) but becomes findable only after latest's run has
      // rebuilt that table and rebound every version it found loaded.
      env = await newEnvironment();
      service = newService(env);
      const resolve = (
         env as unknown as {
            storageBindingResolver: (
               p: string,
               r: ServingReader,
            ) => Promise<unknown>;
         }
      ).storageBindingResolver;
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      let held = false;
      let reached!: () => void;
      const reading = new Promise<void>((resolve) => (reached = resolve));
      env.setStorageBindingResolver(async (packageName, reader) => {
         const entries = await resolve(packageName, reader);
         if (!held) {
            held = true;
            reached();
            await gate;
         }
         return entries as never;
      });
      const loading = env.getPackage(PKG, false, { versionId: "1.0.0" });
      // The only load running is 1.0.0's: the first store read is its.
      await reading;
      await build({});
      await tamper("summary", 202);
      release();
      await loading;

      // `summary` now holds rows 2.0.0's definition built: 1.0.0 serves live.
      const deadline = Date.now() + 5_000;
      while ((await answerOf("1.0.0")) !== 1 && Date.now() < deadline) {
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
      expect(await answerOf("1.0.0")).toBe(1);
      expect(await answerOf("2.0.0")).toBe(202);
   });

   it("a version with the same definition as latest shares latest's table", async () => {
      await publish("1.0.0", 2, "package");
      await publish("2.0.0", 2, "package");
      await build({});
      await tamper("summary", 202);
      expect(await answerOf("1.0.0")).toBe(202);
      expect(await answerOf("2.0.0")).toBe(202);
   });

   it("a run that fails after it starts rebuilding leaves the shared tables unbound for every version", async () => {
      await publish("1.0.0", 1, "package");
      const first = await build({});
      await tamper("summary", 101);
      await publish("2.0.0", 1, "package"); // same definition: it binds
      expect(await answerOf("1.0.0")).toBe(101);
      expect(await answerOf("2.0.0")).toBe(101);

      const failing = spyOn(
         service as unknown as {
            executeInstructedBuild: () => Promise<unknown>;
         },
         "executeInstructedBuild",
      ).mockRejectedValue(new Error("warehouse went away mid-build"));
      const failed = await build({ forceRefresh: true });
      failing.mockRestore();
      expect(failed.status).toBe("FAILED");
      expect(failed.metadata?.writesTables).toEqual(["summary"]);
      expect(
         (await repo.getMaterializationById(first.id))?.metadata?.[
            SUPERSEDED_TABLES_KEY
         ],
      ).toEqual(["summary"]);

      // `summary` may hold anything now: no version reads it.
      expect(await answerOf("1.0.0")).toBe(1);

      env = await newEnvironment();
      service = newService(env);
      expect(await answerOf("1.0.0")).toBe(1);
      expect(await answerOf("2.0.0")).toBe(1);

      // The next run that commits settles it.
      await build({ forceRefresh: true });
      await tamper("summary", 111);
      expect(await answerOf("2.0.0")).toBe(111);
   });

   it("deleting the run that rebuilt a shared table never brings back an older version's binding to it", async () => {
      await publish("1.0.0", 1, "package");
      await build({});
      await publish("2.0.0", 2, "package");
      const latestRun = await build({});
      await tamper("summary", 202);
      expect(await answerOf("1.0.0")).toBe(1);

      // `summary` still holds what 2.0.0's definition built; 1.0.0's run
      // still names it.
      await service.deleteMaterialization(ENV, PKG, latestRun.id);
      expect(await answerOf("1.0.0")).toBe(1);

      env = await newEnvironment();
      service = newService(env);
      expect(await answerOf("1.0.0")).toBe(1);
   });

   it("deleting a run that failed mid-rebuild never brings back an older version's binding", async () => {
      await publish("1.0.0", 1, "package");
      await build({});
      await publish("2.0.0", 2, "package");
      const failing = spyOn(
         service as unknown as {
            executeInstructedBuild: () => Promise<unknown>;
         },
         "executeInstructedBuild",
      ).mockRejectedValue(new Error("warehouse went away mid-build"));
      const failed = await build({});
      failing.mockRestore();
      expect(failed.status).toBe("FAILED");
      // Say it got as far as rebuilding `summary` from 2.0.0's definition.
      await tamper("summary", 202);

      await service.deleteMaterialization(ENV, PKG, failed.id);
      expect(await answerOf("1.0.0")).toBe(1);
      env = await newEnvironment();
      service = newService(env);
      expect(await answerOf("1.0.0")).toBe(1);
   });

   it("a version that cannot be rebound before a rebuild is unloaded, and reads live once it loads again", async () => {
      await publish("1.0.0", 1, "package");
      await build({});
      await tamper("summary", 101);
      expect(await answerOf("1.0.0")).toBe(101);
      await publish("2.0.0", 2, "package");

      const v1 = await env.getPackage(PKG, false, { versionId: "1.0.0" });
      const failing = spyOn(v1, "reloadAllModels").mockRejectedValue(
         new Error("worker pool unavailable"),
      );
      await build({});
      failing.mockRestore();
      await tamper("summary", 202);

      expect(env.getVersionService()!.cache.isLoaded(PKG, "1.0.0")).toBe(false);
      expect(await answerOf("1.0.0")).toBe(1);
   });

   it("a run of another version with instructions never becomes what latest reads", async () => {
      await publish("1.0.0", 1, "package");
      await publish("2.0.0", 2, "package");
      await build({});
      await tamper("summary", 202);
      expect(await answerOf("2.0.0")).toBe(202);

      // 1.0.0's own build, into a table its caller names, committing last.
      const plan = (
         await env.getPackage(PKG, false, { versionId: "1.0.0" })
      ).getBuildPlan()!;
      const source = Object.values(plan.sources)[0];
      await build({
         versionId: "1.0.0",
         buildInstructions: [
            {
               sourceID: source.sourceID,
               sourceEntityId: source.sourceEntityId,
               physicalTableName: "summary_v1_host",
               realization: "COPY",
            },
         ] as never,
      });

      env = await newEnvironment();
      service = newService(env);
      expect(await answerOf("2.0.0")).toBe(202);
   });

   it("never lets two runs write one table at once, nor a run with instructions write the shared tables", async () => {
      await publish("1.0.0", 1, "package");
      await publish("2.0.0", 2, "package");
      await build({});
      const instruct = async (versionId: string, physicalTableName: string) => {
         const plan = (
            await env.getPackage(PKG, false, { versionId })
         ).getBuildPlan()!;
         const source = Object.values(plan.sources)[0];
         return service.createMaterialization(ENV, PKG, {
            versionId,
            buildInstructions: [
               {
                  sourceID: source.sourceID,
                  sourceEntityId: source.sourceEntityId,
                  physicalTableName,
                  realization: "COPY",
               },
            ] as never,
         });
      };

      const shared = await instruct("1.0.0", "summary")
         .then(() => undefined)
         .catch((err: unknown) => err);
      expect(shared).toBeInstanceOf(MaterializationConflictError);

      const release = holdRuns();
      try {
         const first = await instruct("1.0.0", "host_table");
         const second = await instruct("2.0.0", "host_table")
            .then(() => undefined)
            .catch((err: unknown) => err);
         expect(second).toBeInstanceOf(MaterializationConflictError);
         expect((second as Error).message).toContain(first.id);
         release();
         expect((await settled(first.id)).status).toBe("MANIFEST_FILE_READY");
      } finally {
         release();
      }
      // Once it has settled, the table is free again.
      const later = await instruct("2.0.0", "host_table");
      expect((await settled(later.id)).status).toBe("MANIFEST_FILE_READY");
   });

   it("a version bound to a host manifest is left to the host by every rebind", async () => {
      // The host's manifest names no tables: a version bound to it serves live.
      const fetchManifest = spyOn(
         Environment.prototype as unknown as {
            fetchManifestEntriesWithTimeout: () => Promise<unknown>;
         },
         "fetchManifestEntriesWithTimeout",
      ).mockResolvedValue({});
      try {
         const location = writePackage("1.0.0", 1, "package");
         await env.getVersionService()!.publish(
            PKG,
            async (stagingPath) => {
               await fs.promises.cp(location, stagingPath, { recursive: true });
            },
            {
               sourceLocation: location,
               promotion: "on-publish",
               manifestLocation: "gs://bucket/manifest.json",
            },
         );
         await publish("2.0.0", 1, "package"); // the same definition
         await build({});
         await tamper("summary", 111);
         expect(await answerOf("2.0.0")).toBe(111);
         // Neither the post-build rebind nor a restart's load binds it from the
         // local store.
         expect(await answerOf("1.0.0")).toBe(1);
         env = await newEnvironment();
         service = newService(env);
         expect(await answerOf("1.0.0")).toBe(1);
         expect(await answerOf("2.0.0")).toBe(111);
      } finally {
         fetchManifest.mockRestore();
      }
   });

   it("one auto-run per package, while a run with instructions builds a version beside it", async () => {
      await publish("1.0.0", 1, "package");
      await publish("2.0.0", 2, "package");
      const plan = (
         await env.getPackage(PKG, false, { versionId: "1.0.0" })
      ).getBuildPlan()!;
      const source = Object.values(plan.sources)[0];
      const release = holdRuns();
      try {
         const auto = await service.createMaterialization(ENV, PKG, {});
         expect(
            await refusal(service.createMaterialization(ENV, PKG, {})),
         ).toEqual({ status: 409, reason: undefined });
         const instructed = await service.createMaterialization(ENV, PKG, {
            versionId: "1.0.0",
            buildInstructions: [
               {
                  sourceID: source.sourceID,
                  sourceEntityId: source.sourceEntityId,
                  physicalTableName: "summary_host_v1",
                  realization: "COPY",
               },
            ] as never,
         });
         expect([auto.version, instructed.version]).toEqual(["2.0.0", "1.0.0"]);
         release();
         expect((await settled(auto.id)).status).toBe("MANIFEST_FILE_READY");
         expect((await settled(instructed.id)).status).toBe(
            "MANIFEST_FILE_READY",
         );
      } finally {
         release();
      }
      expect(await warehouseTables()).toEqual(["summary", "summary_host_v1"]);
   });

   it("archiving a version reclaims nothing: its tables are the package's", async () => {
      await publish("1.0.0", 1, "package");
      const run = await build({});
      await publish("2.0.0", 1, "package");
      await env.getVersionService()!.setArchiveStatus(PKG, "1.0.0", "archive");
      await Promise.all(reclaims);
      expect(await warehouseTables()).toEqual(["summary"]);
      expect((await repo.getMaterializationById(run.id))?.status).toBe(
         "MANIFEST_FILE_READY",
      );
   });
});

describe("naming a version on the materialization routes", () => {
   it("lists one version's runs, with those from before the first versioned publish", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      const v1 = await build({ versionId: "1.0.0" });
      const v2 = await build({});
      const legacy = await repo.createMaterialization(
         ENV_ID,
         PKG,
         "CANCELLED",
         { mode: "auto" },
      );

      const ids = async (versionId?: unknown) =>
         (await service.listMaterializations(ENV, PKG, { versionId }))
            .map((m) => m.id)
            .sort();
      expect(await ids("1.0.0")).toEqual([v1.id, legacy.id].sort());
      expect(await ids()).toEqual([v2.id, legacy.id].sort());
      expect(await ids("")).toEqual([v2.id, legacy.id].sort());
   });

   it("finds a run by id whichever version built it, and refuses one of another named version", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      const v1 = await build({ versionId: "1.0.0" });

      expect((await service.getMaterialization(ENV, PKG, v1.id)).id).toBe(
         v1.id,
      );
      expect(
         (await service.getMaterialization(ENV, PKG, v1.id, "1.0.0")).id,
      ).toBe(v1.id);
      expect(
         await refusal(service.getMaterialization(ENV, PKG, v1.id, "2.0.0")),
      ).toEqual({ status: 404, reason: undefined });
      expect(
         await refusal(
            service.deleteMaterialization(ENV, PKG, v1.id, {
               versionId: "2.0.0",
            }),
         ),
      ).toEqual({ status: 404, reason: undefined });
      expect(await repo.getMaterializationById(v1.id)).not.toBeNull();
   });

   it("answers a bad, unknown or archived version the way every read does", async () => {
      await publish("1.0.0", 1, "version");
      await publish("2.0.0", 2, "version");
      await env.getVersionService()!.setArchiveStatus(PKG, "1.0.0", "archive");
      for (const [versionId, expected] of [
         ["not-a-version", { status: 400, reason: "VERSION_ID_INVALID" }],
         ["9.9.9", { status: 404, reason: "VERSION_NOT_FOUND" }],
         ["1.0.0", { status: 410, reason: "VERSION_ARCHIVED" }],
      ] as const) {
         expect(
            await refusal(
               service.listMaterializations(ENV, PKG, { versionId }),
            ),
         ).toEqual(expected);
         expect(
            await refusal(
               service.createMaterialization(ENV, PKG, { versionId }),
            ),
         ).toEqual(expected);
         expect(
            await refusal(service.getMaterialization(ENV, PKG, "x", versionId)),
         ).toEqual(expected);
      }
      expect(await repo.listMaterializations(ENV_ID, PKG)).toEqual([]);
   });

   it("lists every run of a versioned package that has no latest yet", async () => {
      await publish("1.0.0", 1, "version", "explicit");
      const run = await build({ versionId: "1.0.0" });
      expect(
         (await service.listMaterializations(ENV, PKG)).map((m) => m.id),
      ).toEqual([run.id]);
   });

   it("refuses a versionId on a package that has no versions", async () => {
      expect(
         await refusal(
            service.listMaterializations(ENV, "unversioned", {
               versionId: "1.0.0",
            }),
         ),
      ).toEqual({ status: 404, reason: "VERSION_NOT_FOUND" });
   });

   it("refuses a create body whose versionId is not text", async () => {
      const controller = new MaterializationController(service);
      for (const versionId of [1, ["1.0.0"], { v: 1 }, true]) {
         const error = await controller
            .createMaterialization(ENV, PKG, { versionId })
            .catch((err: unknown) => err);
         expect(error).toBeInstanceOf(PackageVersionError);
         expect((error as PackageVersionError).reason).toBe(
            "VERSION_ID_INVALID",
         );
      }
   });
});

describe("a package with no versions", () => {
   it("records no version and holds the package's slot, as before", async () => {
      const dir = writePackage("1.0.0", 7, "package");
      fs.cpSync(dir, path.join(envPath, "plain"), { recursive: true });
      const created = await service.createMaterialization(ENV, "plain", {});
      const run = await settled(created.id);
      expect(run.status).toBe("MANIFEST_FILE_READY");
      expect(run.version).toBeNull();
      expect(run.metadata).not.toHaveProperty("versionId");
      expect(run.metadata).not.toHaveProperty("scope");
      expect(
         Object.values(run.manifest?.entries ?? {}).map(
            (e) => e.physicalTableName,
         ),
      ).toEqual(["summary"]);
      await tamper("summary", 70);
      const pkg = await env.getPackage("plain", false);
      expect(pkg).toBeInstanceOf(Package);
      const result = await pkg
         .getModel("model.malloy")!
         .getQueryResults(
            undefined,
            undefined,
            "run: summary -> { select: answer }",
         );
      expect(
         (result as unknown as { compactResult: { answer: number }[] })
            .compactResult[0].answer,
      ).toBe(70);
   });
});

describe("the scheduler, per published version", () => {
   function scheduler(): MaterializationScheduler {
      const store = {
         getLoadedEnvironments: () => [env],
      } as unknown as EnvironmentStore;
      return new MaterializationScheduler(store, service, {
         tickIntervalMs: 60_000,
         maxFiresPerTick: 10,
      });
   }

   async function scheduledRuns(): Promise<Materialization[]> {
      const runs = (await repo.listMaterializations(ENV_ID, PKG)).filter(
         (m) => m.metadata?.trigger === "SCHEDULER",
      );
      return Promise.all(runs.map((m) => settled(m.id)));
   }

   it("loads a version that is due, and builds it into its own tables", async () => {
      await publish("1.0.0", 1, "version", "on-publish", "* * * * *");
      await publish("2.0.0", 2, "version", "on-publish", "* * * * *");
      // A restart: nothing is loaded.
      env = await newEnvironment();
      service = newService(env);
      const sched = scheduler();
      const now = Date.now();

      await sched.tick(now);
      expect(await scheduledRuns()).toEqual([]);
      expect(env.getLoadedVersionIds(PKG)).toEqual([]);

      await sched.tick(now + 120_000);
      const runs = await scheduledRuns();
      expect(runs.map((m) => [m.version, m.status]).sort()).toEqual([
         ["1.0.0", "MANIFEST_FILE_READY"],
         ["2.0.0", "MANIFEST_FILE_READY"],
      ]);
      expect(env.getLoadedVersionIds(PKG).sort()).toEqual(["1.0.0", "2.0.0"]);
      expect(await warehouseTables()).toEqual([
         "summary__v1_0_0",
         "summary__v2_0_0",
      ]);
   });

   it("never fires an archived version, and fires it again once unarchived", async () => {
      await publish("1.0.0", 1, "version", "on-publish", "* * * * *");
      await publish("2.0.0", 2, "version");
      const versions = env.getVersionService()!;
      await versions.setArchiveStatus(PKG, "1.0.0", "archive");
      await Promise.all(reclaims);
      const sched = scheduler();
      const now = Date.now();
      const attempts = spyOn(service, "createMaterialization");

      await sched.tick(now);
      await sched.tick(now + 120_000);
      // Not even tried: it is out of the sweep.
      expect(attempts).not.toHaveBeenCalled();
      expect(await scheduledRuns()).toEqual([]);

      await versions.setArchiveStatus(PKG, "1.0.0", "unarchive");
      await sched.tick(now + 120_001);
      await sched.tick(now + 240_000);
      expect((await scheduledRuns()).map((m) => m.version)).toEqual(["1.0.0"]);
   });
});
