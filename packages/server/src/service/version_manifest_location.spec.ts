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
 * Driven on real versions in a real Environment, over a DuckDB registry.
 */
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
let db: DuckDBConnection;
let repo: ResourceRepository;
let env: Environment;
/** What this server's own run store answers for the package. */
let localStore: Record<string, ManifestEntry>;

beforeEach(async () => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "version-manifest-location-")),
   );
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      [ENV_ID, ENV, envPath],
   );
   repo = createDuckDBRepository(db);
   env = await Environment.create(ENV, envPath, []);
   env.bindVersions(repo, ENV_ID, {
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
   localStore = {};
   env.setStorageBindingResolver(async () => localStore);
});

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

/** `host` (42) and `local` (7) in the version's own DuckDB. */
async function storeTables(pkg: Package): Promise<void> {
   const duckdb = await pkg
      .getMalloyConfig()
      .connections.lookupConnection("duckdb");
   await duckdb.runSQL(
      `CREATE OR REPLACE TABLE summary_host AS SELECT 1 AS a, 42::BIGINT AS n`,
   );
   await duckdb.runSQL(
      `CREATE OR REPLACE TABLE summary_local AS SELECT 1 AS a, 7::BIGINT AS n`,
   );
}

const entryFor = (
   id: string,
   physicalTableName: string,
   dataAsOf?: string,
): ManifestEntry =>
   ({
      sourceEntityId: id,
      sourceName: "summary",
      physicalTableName,
      connectionName: "duckdb",
      ...(dataAsOf ? { dataAsOf } : {}),
   }) as ManifestEntry;

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
   pkg.getBuildManifestEntries()?.[id]?.tableName;

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

   it("keeps serving the host's tables after a local materialization of it", async () => {
      const id = await publishLive("1.0.0");
      const host = writeManifest("host.json", id, "summary_host");
      await versions().setManifest(PKG, "1.0.0", host);
      const pkg = await loadedVersion("1.0.0");
      await storeTables(pkg);
      expect(await total(pkg)).toBe(42);

      // A run on this server builds `summary` and commits its rows.
      const store = {
         storageManager: { getRepository: () => repo },
         getEnvironment: async () => env,
         getLoadedEnvironments: () => [env],
      } as unknown as EnvironmentStore;
      const service = new MaterializationService(store);
      const run = await settled(
         service,
         (
            await service.createMaterialization(ENV, PKG, {
               versionId: VersionRef.parse("1.0.0"),
            })
         ).id,
      );
      expect(run.status).toBe("MANIFEST_FILE_READY");
      const built = (run.manifest as { entries?: Record<string, unknown> })
         ?.entries;
      expect(Object.keys(built ?? {})).toContain(id);
      expect(run.metadata?.autoLoad).toBe("loaded");

      // What the version serves is still the host's.
      expect(pkg.getManifestLocation()).toBe(host);
      expect(tableOf(pkg, id)).toBe('"summary_host"');
      expect(await total(pkg)).toBe(42);
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
      // Naming it again binds nothing.
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
      expect(pkg.getBuildManifestEntries() ?? {}).toEqual({});
      expect(await total(pkg)).toBe(1);
      // And a load afresh stays live.
      const reloaded = await loadAfresh("1.0.0");
      expect(reloaded.getManifestLocation()).toBeNull();
      await storeTables(reloaded);
      expect(await total(reloaded)).toBe(1);
   });
});
