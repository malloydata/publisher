// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A published version's `manifestLocation` is the build manifest its
 * registry row names, through every bind: its publisher.json is immutable
 * content and never says. Two things read it:
 *
 * - `boundManifestOf`, so a publish or `setManifest` naming the manifest a
 *   version already serves binds nothing again;
 * - the local-store paths, so a version bound to a host manifest is never
 *   overlaid with this server's own run-store entries.
 *
 * A version a host has bound is the host's from then on (its row's
 * `hostOwned`): cleared, reloaded or restarted, it serves live until the host
 * binds it again, never what this server's own run store holds. A version no
 * host bound binds its own store on a clear.
 *
 * Driven on real versions in a real Environment, over a DuckDB registry.
 */
import { withNativeSchema } from "../test_helpers/host_manifest";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type {
   ManifestEntry,
   Materialization,
   ResourceRepository,
} from "../storage/DatabaseInterface";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../storage/duckdb/schema";
import { writePackageFiles } from "../test_helpers/temp_package";
import { unrecordedAudit } from "./access_record";
import { Environment } from "./environment";
import type { EnvironmentStore } from "./environment_store";
import { MaterializationService } from "./materialization/run/materialization_service";
import {
   internalErrorToHttpError,
   ManifestBindFailedError,
   MaterializationConflictError,
} from "../errors";
import { parsePackageName, VersionRef } from "./identity";
import { QueryPrivileges } from "./model_query_request";
import type { Package } from "./package";

const ENV = "testEnv";
const ENV_ID = "env-1";
const PKG = "pkg";

const MODEL = `##! experimental.persistence
source: base is duckdb.sql("SELECT 1 AS a")

#@ persist name="summary"
source: summary is base -> {
  group_by: a
  aggregate: n is count()
}
`;

let root: string;
let envPath: string;
let db: DuckDBConnection;
let repo: ResourceRepository;
let env: Environment;
/** What this server's own run store answers for the package. */
let localStore: Record<string, ManifestEntry>;

beforeEach(async () => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "version-manifest-location-")),
   );
   envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      [ENV_ID, ENV, envPath],
   );
   repo = createDuckDBRepository(db);
   localStore = {};
   env = await bootEnvironment();
});

/**
 * An Environment over the test's registry and folder, as a server boots one:
 * a second call is a restart, which reads every version from its row.
 */
async function bootEnvironment(): Promise<Environment> {
   const env = await Environment.create(ENV, envPath, []);
   env.bindVersions(repo, ENV_ID, {
      downloaderFor: (_packageName, location) => async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
   });
   env.setStorageBindingResolver(async () => localStore);
   return env;
}

afterEach(async () => {
   await env.closeAllConnections().catch(() => undefined);
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
});

const versions = () => env.getVersionService()!;

function source(version: string): string {
   const dir = path.join(root, "sources", version);
   writePackageFiles(
      dir,
      { "m.malloy": MODEL },
      { manifest: { name: PKG, version } },
   );
   return dir;
}

function publish(version: string, manifestLocation?: string) {
   const location = source(version);
   return versions().publish(
      PKG,
      async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      {
         sourceLocation: location,
         promotion: "on-publish",
         ...(manifestLocation ? { manifestLocation } : {}),
      },
   );
}

const loadedVersion = (versionId: string): Promise<Package> =>
   env.getPackage(parsePackageName(PKG), false, {
      versionId: VersionRef.parse(versionId),
   });

const rowOf = async (versionId: string) =>
   (await versions().getVersion(PKG, versionId)).manifestPath;

/** `summary`'s total `n`: 1 live, else the `n` of the table it is bound to. */
async function total(pkg: Package): Promise<number> {
   const res = await pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query: "run: summary -> { aggregate: t is n.sum() }" },
         QueryPrivileges.of({ bypassFilters: true }),
      );
   return Number((res.compactResult as unknown as { t: number }[])[0].t);
}

/**
 * `host` (42) and `local` (7) in the version's own DuckDB, once the retry has
 * read them. The version bound them before they existed, so its bind could
 * not read them: that is asserted first, before they are created, so it
 * holds whatever the retry's timing.
 */
async function storeTables(pkg: Package, unread = true): Promise<void> {
   const unreadWarnings = () =>
      (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === "database-table-unread",
      );
   expect(pkg.materialization().hasUnreadDatabaseTables()).toBe(unread);
   if (!unread) {
      // Nothing to warn of, and nothing a poll would rebind for.
      expect(unreadWarnings()).toEqual([]);
   }
   // Unread, and the package says so, with a fixed reason and a pending
   // retry, never the driver's text.
   const [warning] = unread ? unreadWarnings() : [];
   if (unread)
      expect(warning).toMatchObject({
         code: "database-table-unread",
         severity: "warn",
         subject: "summary",
      });
   if (unread) {
      // DuckDB's DESCRIBE of a missing table fails: a fixed class.
      expect(warning.message).toContain("(the catalog query failed)");
      expect(warning.message).toContain("A retry is pending.");
      expect(warning.message).not.toContain("Catalog Error");
   }
   const duckdb = await pkg
      .getMalloyConfig()
      .connections.lookupConnection("duckdb");
   await duckdb.runSQL(
      `CREATE OR REPLACE TABLE summary_host AS SELECT 1 AS a, 42::BIGINT AS n`,
   );
   await duckdb.runSQL(
      `CREATE OR REPLACE TABLE summary_local AS SELECT 1 AS a, 7::BIGINT AS n`,
   );
   // No rebind here: the facade's retry is what is proven.
   const deadline = Date.now() + 15_000;
   while (pkg.materialization().hasUnreadDatabaseTables()) {
      if (Date.now() > deadline) {
         throw new Error("the unread tables were never read again");
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
   }
   // Read: the warning clears.
   expect(unreadWarnings()).toEqual([]);
}

/** Each summary table's columns, as its build records them (host_manifest.ts). */
const SUMMARY_TYPES = [
   { name: "a", type: "INTEGER" },
   { name: "n", type: "BIGINT" },
];

const entryFor = (
   id: string,
   physicalTableName: string,
   dataAsOf?: string,
): ManifestEntry =>
   withNativeSchema(
      {
         sourceEntityId: id,
         sourceName: "summary",
         physicalTableName,
         connectionName: "duckdb",
         ...(dataAsOf ? { dataAsOf } : {}),
      },
      SUMMARY_TYPES,
   ) as ManifestEntry;

/**
 * Publish `version` live and return `summary`'s source entity id in it (a
 * version names its own).
 */
async function publishLive(version: string): Promise<string> {
   await publish(version);
   const id = Object.values(
      (await loadedVersion(version)).getBuildPlan()?.sources ?? {},
   )[0]?.sourceEntityId;
   expect(id).toBeString();
   return id!;
}

/** Drop the version from memory, so the next read loads it from its row. */
async function loadAfresh(version: string): Promise<Package> {
   versions().cache.evict(PKG, version);
   return loadedVersion(version);
}

/** A host manifest binding `summary` to `table`, written to `name`. */
function writeManifest(
   name: string,
   id: string,
   table: string,
   dataAsOf?: string,
): string {
   const file = path.join(root, name);
   fs.writeFileSync(
      file,
      JSON.stringify({
         builtAt: new Date().toISOString(),
         strict: false,
         entries: { [id]: entryFor(id, table, dataAsOf) },
      }),
   );
   return file;
}

const tableOf = (pkg: Package, id: string) =>
   pkg.materialization().getBuildManifestEntries()?.[id]?.tableName;

/** A MaterializationService over `env`, as main_server.ts wires it. */
function materializationService(): MaterializationService {
   const store = {
      storageManager: { getRepository: () => repo },
      getEnvironment: async () => env,
      getLoadedEnvironments: () => [env],
   } as unknown as EnvironmentStore;
   return new MaterializationService(store);
}

/** A run, once it has ended and its service stopped tracking it. */
async function settled(
   service: MaterializationService,
   id: string,
): Promise<Materialization> {
   const deadline = Date.now() + 60_000;
   const running = (
      service as unknown as { runningAbortControllers: Map<string, unknown> }
   ).runningAbortControllers;
   for (;;) {
      const m = await repo.getMaterializationById(id);
      if (
         m &&
         ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status) &&
         !running.has(id)
      ) {
         return m;
      }
      if (Date.now() > deadline) throw new Error(`run ${id} never settled`);
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
}

/** A host's instructed run of `version`, building `summary` into `table`, once settled. */
async function instructedRun(
   version: string,
   id: string,
   table: string,
): Promise<Materialization> {
   const pkg = await loadedVersion(version);
   const source = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
      (s) => s.sourceEntityId === id,
   )!;
   const service = materializationService();
   return settled(
      service,
      (
         await service.createMaterialization(ENV, PKG, {
            versionId: VersionRef.parse(version),
            buildInstructions: [
               {
                  sourceID: source.sourceID,
                  sourceEntityId: id,
                  physicalTableName: table,
                  realization: "COPY",
               },
            ] as never,
         })
      ).id,
   );
}

type Rebinder = {
   bindManifest(pkg: Package, uri: string): Promise<void>;
   bindVersionManifest(pkg: Package, uri: string | null): Promise<void>;
};
const internals = () => env as unknown as Rebinder;

describe("a version's manifestLocation is its registry row's", () => {
   it("after a load, a table-changing rebind, a freshness-only rebind and an identical rebind", async () => {
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      await versions().setManifest(PKG, "1.0.0", host);
      // A load, compiled over the row's manifest and bound to it.
      const pkg = await loadAfresh("1.0.0");
      expect(await rowOf("1.0.0")).toBe(host);
      expect(tableOf(pkg, id)).toBe('"summary_host"');
      expect(pkg.getManifestLocation()).toBe(host);

      // A table-changing rebind recompiles the models.
      const other = writeManifest("other.json", id, "summary_local");
      await versions().setManifest(PKG, "1.0.0", other);
      expect(await rowOf("1.0.0")).toBe(other);
      expect(tableOf(pkg, id)).toBe('"summary_local"');
      expect(pkg.getManifestLocation()).toBe(other);

      // A freshness-only rebind binds without a compile.
      const fresher = writeManifest(
         "fresher.json",
         id,
         "summary_local",
         "2026-11-02T00:00:00Z",
      );
      await versions().setManifest(PKG, "1.0.0", fresher);
      expect(await rowOf("1.0.0")).toBe(fresher);
      expect(pkg.getManifestLocation()).toBe(fresher);

      // The identical manifest again.
      await internals().bindManifest(pkg, fresher);
      expect(pkg.getManifestLocation()).toBe(fresher);
      expect(await rowOf("1.0.0")).toBe(fresher);
   });

   it("is never overlaid from the local run store when bound to a host manifest", async () => {
      const id = await publishLive("1.0.0");
      // This server's own runs hold an entry for the same source.
      localStore = { [id]: entryFor(id, "summary_local") };
      const host = writeManifest("host.json", id, "summary_host");
      await versions().setManifest(PKG, "1.0.0", host);
      const resolver = spyOn(
         env as unknown as {
            storageBindingResolver: () => Promise<unknown>;
         },
         "storageBindingResolver",
      );

      // A load bound to the host manifest.
      const pkg = await loadAfresh("1.0.0");
      await storeTables(pkg);
      expect(tableOf(pkg, id)).toBe('"summary_host"');
      expect(await total(pkg)).toBe(42);

      // A freshness-only rebind, then a rebind from the store.
      const fresher = writeManifest(
         "fresher.json",
         id,
         "summary_host",
         "2026-11-02T00:00:00Z",
      );
      await versions().setManifest(PKG, "1.0.0", fresher);
      await env.rebindVersionFromStore(PKG, "1.0.0", async () => localStore);
      expect(tableOf(pkg, id)).toBe('"summary_host"');
      expect(await total(pkg)).toBe(42);
      // The host-bound version never asked the local store.
      expect(resolver).not.toHaveBeenCalled();
   });

   it("does not serve a run's tables when a host manifest is bound while it builds", async () => {
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      const service = materializationService();
      // The manifest is bound after the run is created, before it loads.
      const internal = env as unknown as {
         loadRunManifest: Environment["loadRunManifest"];
      };
      const original = internal.loadRunManifest.bind(env);
      spyOn(internal, "loadRunManifest").mockImplementation(async (params) => {
         await versions().setManifest(PKG, "1.0.0", host);
         return original(params);
      });
      const run = await settled(
         service,
         (
            await service.createMaterialization(ENV, PKG, {
               versionId: VersionRef.parse("1.0.0"),
            })
         ).id,
      );
      // Built and committed, but not served, as a load that did not take.
      expect(run.status).toBe("MANIFEST_FILE_READY");
      const built = (run.manifest as { entries?: Record<string, unknown> })
         ?.entries;
      expect(Object.keys(built ?? {})).toContain(id);
      expect(run.metadata?.autoLoad).toBe("failed");
      expect(String(run.metadata?.autoLoadError)).toContain("host manifest");

      // What the version serves is the host's.
      const pkg = await loadedVersion("1.0.0");
      await storeTables(pkg);
      expect(pkg.getManifestLocation()).toBe(host);
      expect(tableOf(pkg, id)).toBe('"summary_host"');
      expect(await total(pkg)).toBe(42);
   });

   it("serves live once a host clears its manifest, never the local store, through a reload and a restart", async () => {
      const id = await publishLive("1.0.0");
      // This server's own store holds a table for the source.
      localStore = { [id]: entryFor(id, "summary_local") };
      const host = writeManifest("host.json", id, "summary_host");
      await versions().setManifest(PKG, "1.0.0", host);
      const pkg = await loadedVersion("1.0.0");
      await storeTables(pkg);
      expect(await total(pkg)).toBe(42);
      expect((await versions().getVersion(PKG, "1.0.0")).hostOwned).toBe(true);

      await versions().setManifest(PKG, "1.0.0", null);
      expect(pkg.getManifestLocation()).toBeNull();
      expect(tableOf(pkg, id)).toBeUndefined();
      expect(await total(pkg)).toBe(1);
      // Reported as not bound, as a version with nothing bound is, and the
      // row names no manifest but keeps the mark.
      expect(pkg.getOperationalFields()).toMatchObject({
         manifestBindingStatus: "unbound",
         manifestEntryCount: 0,
         boundManifestUri: null,
      });
      expect(await versions().getVersion(PKG, "1.0.0")).toMatchObject({
         manifestPath: null,
         hostOwned: true,
      });

      // The host drops the tables it retired: nothing reads them.
      const duckdb = await pkg
         .getMalloyConfig()
         .connections.lookupConnection("duckdb");
      await duckdb.runSQL("DROP TABLE summary_local");
      await duckdb.runSQL("DROP TABLE summary_host");
      expect(await total(pkg)).toBe(1);

      const resolver = spyOn(
         env as unknown as {
            storageBindingResolver: () => Promise<unknown>;
         },
         "storageBindingResolver",
      );
      // A rebind from the store leaves it live.
      await env.rebindVersionFromStore(PKG, "1.0.0", async () => localStore);
      expect(tableOf(pkg, id)).toBeUndefined();
      expect(await total(pkg)).toBe(1);

      // A reload: loaded afresh from its row, it stays live.
      const reloaded = await loadAfresh("1.0.0");
      expect(reloaded.getManifestLocation()).toBeNull();
      expect(tableOf(reloaded, id)).toBeUndefined();
      expect(reloaded.materialization().hasUnreadDatabaseTables()).toBe(false);
      expect(await total(reloaded)).toBe(1);
      expect(reloaded.getOperationalFields()).toMatchObject({
         manifestBindingStatus: "unbound",
         boundManifestUri: null,
      });
      expect(resolver).not.toHaveBeenCalled();

      // A restart: a new Environment reads the mark from the row.
      await env.closeAllConnections();
      env = await bootEnvironment();
      const restarted = await loadedVersion("1.0.0");
      expect(restarted.getManifestLocation()).toBeNull();
      expect(tableOf(restarted, id)).toBeUndefined();
      expect(await total(restarted)).toBe(1);
      expect(restarted.getOperationalFields()).toMatchObject({
         manifestBindingStatus: "unbound",
         boundManifestUri: null,
      });

      // Until the host binds it again.
      await storeTables(restarted, false);
      await versions().setManifest(PKG, "1.0.0", host);
      expect(tableOf(restarted, id)).toBe('"summary_host"');
      expect(await total(restarted)).toBe(42);
   });

   it("reports not bound once a host clears an empty manifest", async () => {
      const id = await publishLive("1.0.0");
      localStore = { [id]: entryFor(id, "summary_local") };
      const empty = path.join(root, "empty.json");
      fs.writeFileSync(
         empty,
         JSON.stringify({
            builtAt: new Date().toISOString(),
            strict: false,
            entries: {},
         }),
      );
      await versions().setManifest(PKG, "1.0.0", empty);
      const pkg = await loadedVersion("1.0.0");
      expect(pkg.getOperationalFields()).toMatchObject({
         manifestBindingStatus: "bound",
         boundManifestUri: empty,
      });

      await versions().setManifest(PKG, "1.0.0", null);
      expect(pkg.getOperationalFields()).toMatchObject({
         manifestBindingStatus: "unbound",
         manifestEntryCount: 0,
         boundManifestUri: null,
      });
      expect(tableOf(pkg, id)).toBeUndefined();
      expect(await total(pkg)).toBe(1);
   });

   it("binds the local store on a clear when no host bound it", async () => {
      const id = await publishLive("1.0.0");
      localStore = { [id]: entryFor(id, "summary_local") };
      const pkg = await loadAfresh("1.0.0");
      await storeTables(pkg);
      expect(await total(pkg)).toBe(7);

      await versions().setManifest(PKG, "1.0.0", null);
      expect(await versions().getVersion(PKG, "1.0.0")).toMatchObject({
         manifestPath: null,
         hostOwned: false,
      });
      expect(tableOf(pkg, id)).toBe('"summary_local"');
      expect(await total(pkg)).toBe(7);
   });

   it("is bound from the local run store when bound to no manifest", async () => {
      const id = await publishLive("1.0.0");
      localStore = { [id]: entryFor(id, "summary_local") };
      const pkg = await loadAfresh("1.0.0");
      await storeTables(pkg);
      expect(pkg.getManifestLocation()).toBeNull();
      expect(await total(pkg)).toBe(7);
   });

   it("binds once when a publish or setManifest names the manifest already served", async () => {
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      const binds = spyOn(internals(), "bindVersionManifest");

      // Resident: the publish binds it once, and the read that follows sees
      // it bound.
      await publish("1.0.0", host);
      expect(binds).toHaveBeenCalledTimes(1);

      // Not resident: the load binds the row's manifest, so neither a
      // publish nor a setManifest naming it binds again.
      const other = writeManifest("other.json", id, "summary_local");
      versions().cache.evict(PKG, "1.0.0");
      binds.mockClear();
      await publish("1.0.0", other);
      expect(binds).not.toHaveBeenCalled();
      const fresher = writeManifest(
         "fresher.json",
         id,
         "summary_local",
         "2026-11-02T00:00:00Z",
      );
      versions().cache.evict(PKG, "1.0.0");
      await versions().setManifest(PKG, "1.0.0", fresher);
      expect(binds).not.toHaveBeenCalled();
      expect((await loadedVersion("1.0.0")).getManifestLocation()).toBe(
         fresher,
      );
   });

   it("clears with a setManifest to none, serving live", async () => {
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      await versions().setManifest(PKG, "1.0.0", host);
      const pkg = await loadedVersion("1.0.0");
      await storeTables(pkg);
      expect(await total(pkg)).toBe(42);

      await versions().setManifest(PKG, "1.0.0", null);
      expect(await rowOf("1.0.0")).toBeNull();
      expect(pkg.getManifestLocation()).toBeNull();
      expect(pkg.materialization().getBuildManifestEntries() ?? {}).toEqual({});
      expect(await total(pkg)).toBe(1);
      // And a load afresh stays live.
      const reloaded = await loadAfresh("1.0.0");
      expect(reloaded.getManifestLocation()).toBeNull();
      // Not unread: nothing is bound.
      await storeTables(reloaded, false);
      expect(await total(reloaded)).toBe(1);
   });
});

describe("a local run of a host-bound package or version", () => {
   it("is refused with a 409 for a version bound to a host manifest, before anything is built", async () => {
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      await versions().setManifest(PKG, "1.0.0", host);
      const refused = await materializationService()
         .createMaterialization(ENV, PKG, {
            versionId: VersionRef.parse("1.0.0"),
         })
         .catch((error: unknown) => error);
      expect(refused).toBeInstanceOf(MaterializationConflictError);
      expect(internalErrorToHttpError(refused as Error).status).toBe(409);
      expect(internalErrorToHttpError(refused as Error).json.reason).toBe(
         "MATERIALIZATION_CONFLICT",
      );
      expect((refused as Error).message).toContain(
         "a local run would build tables it cannot serve",
      );
      expect(await repo.listMaterializations(ENV_ID, PKG)).toEqual([]);
   });

   it("refuses instructions that name one table twice with a 400, BUILD_INSTRUCTIONS_INVALID", async () => {
      const id = await publishLive("1.0.0");
      const pkg = await loadedVersion("1.0.0");
      const source = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
         (s) => s.sourceEntityId === id,
      )!;
      const instruction = {
         sourceID: source.sourceID,
         sourceEntityId: id,
         physicalTableName: "summary_twice",
         realization: "COPY",
      };
      const refused = await materializationService()
         .createMaterialization(ENV, PKG, {
            versionId: VersionRef.parse("1.0.0"),
            buildInstructions: [instruction, instruction] as never,
         })
         .then(
            () => new Error("not refused"),
            (error: unknown) => error as Error,
         );
      expect(refused.message).toContain("more than once");
      expect(internalErrorToHttpError(refused).json).toMatchObject({
         code: 400,
         reason: "BUILD_INSTRUCTIONS_INVALID",
      });
      expect(await repo.listMaterializations(ENV_ID, PKG)).toEqual([]);
   });

   it("is refused with a 409 for an unversioned package bound to a host manifest", async () => {
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      writePackageFiles(
         path.join(root, "env", "solo"),
         { "m.malloy": MODEL },
         { manifest: { name: "solo", manifestLocation: host } },
      );
      const refused = await materializationService()
         .createMaterialization(ENV, "solo", {})
         .catch((error: unknown) => error);
      expect(refused).toBeInstanceOf(MaterializationConflictError);
      expect(internalErrorToHttpError(refused as Error).status).toBe(409);
      expect(internalErrorToHttpError(refused as Error).json.reason).toBe(
         "MATERIALIZATION_CONFLICT",
      );
      expect((refused as Error).message).toContain("Package solo is bound");
   });

   it("builds a host's instructed run of a cleared host-owned version, and serves it live, through a restart", async () => {
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      await versions().setManifest(PKG, "1.0.0", host);
      await versions().setManifest(PKG, "1.0.0", null);

      // Accepted (it has buildInstructions), built, and not served: an
      // instructed run never loads its own tables.
      const run = await instructedRun("1.0.0", id, "summary_built");
      expect(run.status).toBe("MANIFEST_FILE_READY");
      const pkg = await loadedVersion("1.0.0");
      expect(tableOf(pkg, id)).toBeUndefined();
      expect(await total(pkg)).toBe(1);

      // The store now holds the run's table; a restart still serves live.
      localStore = { [id]: entryFor(id, "summary_built") };
      await env.closeAllConnections();
      env = await bootEnvironment();
      const restarted = await loadedVersion("1.0.0");
      expect(tableOf(restarted, id)).toBeUndefined();
      expect(await total(restarted)).toBe(1);
   });

   it("builds a host's instructed run of a version still bound to a host manifest, which keeps serving the host's table, through a restart", async () => {
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      await versions().setManifest(PKG, "1.0.0", host);
      const pkg = await loadedVersion("1.0.0");
      await storeTables(pkg);
      expect(await total(pkg)).toBe(42);

      const run = await instructedRun("1.0.0", id, "summary_built");
      expect(run.status).toBe("MANIFEST_FILE_READY");
      expect(tableOf(pkg, id)).toBe('"summary_host"');
      expect(await total(pkg)).toBe(42);

      localStore = { [id]: entryFor(id, "summary_built") };
      await env.closeAllConnections();
      env = await bootEnvironment();
      const restarted = await loadedVersion("1.0.0");
      await storeTables(restarted);
      expect(tableOf(restarted, id)).toBe('"summary_host"');
      expect(await total(restarted)).toBe(42);
   });

   it("makes a version a host built host-owned, though no host bound it", async () => {
      const id = await publishLive("1.0.0");
      expect((await versions().getVersion(PKG, "1.0.0")).hostOwned).toBe(false);
      const run = await instructedRun("1.0.0", id, "summary_built");
      expect(run.status).toBe("MANIFEST_FILE_READY");
      expect((await versions().getVersion(PKG, "1.0.0")).hostOwned).toBe(true);

      // Its build is in the store; a clear serves live, never it, and a
      // local run is refused.
      localStore = { [id]: entryFor(id, "summary_built") };
      const pkg = await loadedVersion("1.0.0");
      await versions().setManifest(PKG, "1.0.0", null);
      expect(tableOf(pkg, id)).toBeUndefined();
      expect(await total(pkg)).toBe(1);
      const refused = await materializationService()
         .createMaterialization(ENV, PKG, {
            versionId: VersionRef.parse("1.0.0"),
         })
         .catch((error: unknown) => error);
      expect(refused).toBeInstanceOf(MaterializationConflictError);
   });

   it("takes a loaded version serving its local store live when a host's instructed run of it commits", async () => {
      const id = await publishLive("1.0.0");
      localStore = { [id]: entryFor(id, "summary_local") };
      const pkg = await loadAfresh("1.0.0");
      await storeTables(pkg);
      expect(tableOf(pkg, id)).toBe('"summary_local"');
      expect(await total(pkg)).toBe(7);

      const run = await instructedRun("1.0.0", id, "summary_built");
      expect(run.status).toBe("MANIFEST_FILE_READY");
      // The same loaded copy: live, and reported so.
      expect(await loadedVersion("1.0.0")).toBe(pkg);
      expect(env.isHostOwned(pkg)).toBe(true);
      expect(tableOf(pkg, id)).toBeUndefined();
      expect(await total(pkg)).toBe(1);
      expect(pkg.getOperationalFields()).toMatchObject({
         manifestBindingStatus: "unbound",
         boundManifestUri: null,
      });
      expect((await versions().getVersion(PKG, "1.0.0")).hostOwned).toBe(true);
      // A rebind from the store, which now holds the run's table, leaves it.
      localStore = { [id]: entryFor(id, "summary_built") };
      await env.rebindVersionFromStore(PKG, "1.0.0", async () => localStore);
      expect(tableOf(pkg, id)).toBeUndefined();
      expect(await total(pkg)).toBe(1);
   });

   it("does not make a version host-owned when its manifest never binds", async () => {
      const id = await publishLive("1.0.0");
      localStore = { [id]: entryFor(id, "summary_local") };
      const pkg = await loadAfresh("1.0.0");
      await storeTables(pkg);
      expect(await total(pkg)).toBe(7);

      // A mistyped manifest: its fetch fails, so it is refused and nothing
      // of it is applied, the name it gave included.
      const refused = await versions()
         .setManifest(PKG, "1.0.0", path.join(root, "no-such-manifest.json"))
         .catch((error: unknown) => error);
      expect(refused).toBeInstanceOf(ManifestBindFailedError);
      expect(internalErrorToHttpError(refused as Error).json).toMatchObject({
         code: 400,
         reason: "MANIFEST_NOT_FOUND",
      });
      expect((await versions().getVersion(PKG, "1.0.0")).hostOwned).toBe(false);
      expect(await rowOf("1.0.0")).toBeNull();
      expect(env.isHostOwned(pkg)).toBe(false);
      expect(tableOf(pkg, id)).toBe('"summary_local"');
      expect(await total(pkg)).toBe(7);

      // Cleared, it is the standalone version it was: its store serves.
      await versions().setManifest(PKG, "1.0.0", null);
      expect(env.isHostOwned(pkg)).toBe(false);
      expect(tableOf(pkg, id)).toBe('"summary_local"');
      expect(await total(pkg)).toBe(7);
   });

   it("does not count a run of an unloaded host-owned version as loaded", async () => {
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      await versions().setManifest(PKG, "1.0.0", host);
      await versions().setManifest(PKG, "1.0.0", null);
      versions().cache.evict(PKG, "1.0.0");
      const outcome = await env.loadRunManifest({
         packageName: PKG,
         versionId: "1.0.0",
         database: {},
         storage: {},
         commit: async () => undefined,
      });
      expect(outcome).toMatchObject({ loaded: false, step: "bind" });
   });

   it("is refused with a 409 once a host has bound the version, after it clears the manifest too", async () => {
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      await versions().setManifest(PKG, "1.0.0", host);
      await versions().setManifest(PKG, "1.0.0", null);
      const refused = await materializationService()
         .createMaterialization(ENV, PKG, {
            versionId: VersionRef.parse("1.0.0"),
         })
         .catch((error: unknown) => error);
      expect(refused).toBeInstanceOf(MaterializationConflictError);
      expect(internalErrorToHttpError(refused as Error).status).toBe(409);
      expect(internalErrorToHttpError(refused as Error).json.reason).toBe(
         "MATERIALIZATION_CONFLICT",
      );
      expect((refused as Error).message).toContain(
         "has been bound or built by a host",
      );
      expect(await repo.listMaterializations(ENV_ID, PKG)).toEqual([]);
   });

   it("runs, and serves what it built, for a version no host bound", async () => {
      const id = await publishLive("1.0.0");
      const service = materializationService();
      const run = await settled(
         service,
         (
            await service.createMaterialization(ENV, PKG, {
               versionId: VersionRef.parse("1.0.0"),
            })
         ).id,
      );
      expect(run.status).toBe("MANIFEST_FILE_READY");
      expect(run.metadata?.autoLoad).toBe("loaded");
      const pkg = await loadedVersion("1.0.0");
      expect(tableOf(pkg, id)).toBeString();
   });

   it("serves what the local store holds as soon as an unversioned package's manifest is cleared, where it restores from the store", async () => {
      env.restoreUnversionedBindingsFromLocalStore(true);
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      writePackageFiles(
         path.join(root, "env", "solo"),
         { "m.malloy": MODEL },
         { manifest: { name: "solo", manifestLocation: host } },
      );
      const solo = await env.getPackage(parsePackageName("solo"), false);
      const soloId = Object.values(solo.getBuildPlan()?.sources ?? {})[0]
         ?.sourceEntityId as string;
      localStore = { [soloId]: entryFor(soloId, "summary_local") };
      // Not unread: its host table is another definition's: no source here reads it.
      await storeTables(solo, false);
      expect(solo.getManifestLocation()).toBe(host);

      await env.updatePackage("solo", { name: "solo", manifestLocation: null });
      expect(solo.getManifestLocation()).toBeNull();
      expect(tableOf(solo, soloId)).toBe('"summary_local"');
      expect(await total(solo)).toBe(7);
   });

   it("goes live when an unversioned package's manifest is cleared, where it does not restore from the store", async () => {
      // An environment created at runtime: unversioned packages serve live
      // until a build binds them, a clear included.
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      writePackageFiles(
         path.join(root, "env", "solo"),
         { "m.malloy": MODEL },
         { manifest: { name: "solo", manifestLocation: host } },
      );
      const solo = await env.getPackage(parsePackageName("solo"), false);
      const soloId = Object.values(solo.getBuildPlan()?.sources ?? {})[0]
         ?.sourceEntityId as string;
      localStore = { [soloId]: entryFor(soloId, "summary_local") };
      const resolver = spyOn(
         env as unknown as {
            storageBindingResolver: () => Promise<unknown>;
         },
         "storageBindingResolver",
      );
      // Not unread: its host table is another definition's: no source here reads it.
      await storeTables(solo, false);

      await env.updatePackage("solo", { name: "solo", manifestLocation: null });
      expect(solo.getManifestLocation()).toBeNull();
      expect(tableOf(solo, soloId)).toBeUndefined();
      expect(await total(solo)).toBe(1);
      expect(resolver).not.toHaveBeenCalled();
   });
});
