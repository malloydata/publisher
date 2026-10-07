// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { internalErrorToHttpError, PackageVersionError } from "../errors";
import type { PackageVersion } from "../storage/DatabaseInterface";
import { Environment, type VersionRegistry } from "./environment";
import { hashPackageTree } from "./package_content_hash";

function version(
   packageName: string,
   v: string,
   overrides: Partial<PackageVersion> = {},
): PackageVersion {
   return {
      id: `${packageName}-${v}`,
      environmentId: "env-id",
      packageName,
      version: v,
      dirName: v.replace(/\+/g, "_"),
      contentHash: `hash-${v}`,
      sourceLocation: null,
      manifestLocation: null,
      archiveStatus: "unarchive",
      archivedAt: null,
      description: null,
      gitCommitSha: null,
      gitRef: null,
      createdAt: new Date("2026-10-01T00:00:00Z"),
      updatedAt: new Date("2026-10-01T00:00:00Z"),
      ...overrides,
   };
}

function refusal(fn: () => unknown): { status: number; reason?: string } {
   try {
      fn();
   } catch (error) {
      expect(error).toBeInstanceOf(PackageVersionError);
      const { status, json } = internalErrorToHttpError(error as Error);
      return { status, reason: (json as { reason?: string }).reason };
   }
   throw new Error("expected a refusal");
}

describe("Environment.resolveSlot", () => {
   let rootDir: string;
   let envPath: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-slots-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   describe("an unversioned package", () => {
      it("resolves to its single slot, keyed and placed exactly as before versions", () => {
         expect(env.resolveSlot("sales")).toEqual({
            name: "sales",
            key: "sales",
            path: path.join(path.resolve(envPath), "sales"),
         });
         expect(env.isVersionedPackage("sales")).toBe(false);
         expect(env.listPackageVersions("sales")).toEqual({
            latest: null,
            versions: [],
         });
      });

      it("refuses a named version with 404 VERSION_NOT_FOUND rather than serving its only tree", () => {
         expect(refusal(() => env.resolveSlot("sales", "1.0.0"))).toEqual({
            status: 404,
            reason: "VERSION_NOT_FOUND",
         });
      });
   });

   describe("a versioned package", () => {
      beforeEach(() => {
         env.setPackageVersions("sales", "1.1.0", [
            version("sales", "1.0.0"),
            version("sales", "1.1.0"),
            version("sales", "2.0.0-rc1+build.7"),
            version("sales", "0.9.0", { archiveStatus: "archive" }),
         ]);
      });

      it("resolves an omitted version to latest", () => {
         const slot = env.resolveSlot("sales");
         expect(slot.version?.version).toBe("1.1.0");
         expect(slot.key).toBe("sales@1.1.0");
         expect(slot.path).toBe(
            path.join(path.resolve(envPath), "sales", "1.1.0"),
         );
      });

      it("resolves a named version, under its directory name", () => {
         const slot = env.resolveSlot("sales", "2.0.0-rc1+build.7");
         expect(slot.key).toBe("sales@2.0.0-rc1_build.7");
         expect(slot.path).toBe(
            path.join(path.resolve(envPath), "sales", "2.0.0-rc1_build.7"),
         );
      });

      it("refuses an unknown version with 404 VERSION_NOT_FOUND", () => {
         expect(refusal(() => env.resolveSlot("sales", "3.0.0"))).toEqual({
            status: 404,
            reason: "VERSION_NOT_FOUND",
         });
      });

      it("refuses an archived version with 410 VERSION_ARCHIVED", () => {
         expect(refusal(() => env.resolveSlot("sales", "0.9.0"))).toEqual({
            status: 410,
            reason: "VERSION_ARCHIVED",
         });
      });

      it("refuses an omitted version when the package has no latest", () => {
         env.setPackageVersions("sales", null, [version("sales", "1.0.0")]);
         expect(refusal(() => env.resolveSlot("sales"))).toEqual({
            status: 404,
            reason: "VERSION_NOT_FOUND",
         });
         expect(env.resolveSlot("sales", "1.0.0").key).toBe("sales@1.0.0");
      });

      it("lists versions highest first, archived ones included", () => {
         expect(
            env.listPackageVersions("sales").versions.map((v) => v.version),
         ).toEqual(["2.0.0-rc1+build.7", "1.1.0", "1.0.0", "0.9.0"]);
         expect(env.listPackageVersions("sales").latest).toBe("1.1.0");
      });

      it("goes back to a single unversioned slot once its versions are cleared", () => {
         env.clearPackageVersions("sales");
         expect(env.resolveSlot("sales").key).toBe("sales");
      });
   });

   it("refuses a package name that is not a safe path segment", () => {
      expect(() => env.resolveSlot("../escape")).toThrow();
   });
});

/** An in-memory registry with the behaviour the environment relies on. */
function memoryRegistry(): VersionRegistry & {
   rows: PackageVersion[];
   latest: Map<string, string | null>;
} {
   const rows: PackageVersion[] = [];
   const latest = new Map<string, string | null>();
   return {
      rows,
      latest,
      listAllVersions: async () => [...rows],
      listVersions: async (name) => rows.filter((r) => r.packageName === name),
      getLatest: async (name) => latest.get(name) ?? null,
      ensurePackage: async (name) => {
         if (!latest.has(name)) latest.set(name, null);
      },
      createVersion: async (v) => {
         const row = version(v.packageName, v.version, { ...v });
         rows.push(row);
         return row;
      },
      setLatest: async (name, expected, next) => {
         if ((latest.get(name) ?? null) !== expected) return false;
         latest.set(name, next);
         return true;
      },
      updateVersion: async (id, updates) => {
         const row = rows.find((r) => r.id === id);
         if (!row) throw new Error("no such version");
         Object.assign(row, updates);
         return row;
      },
   };
}

describe("Environment versions from the registry", () => {
   let rootDir: string;
   let envPath: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-vreg-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   /** A package tree on disk whose model answers `n`. */
   async function writeTree(dir: string, n: number): Promise<void> {
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "sales", version: "1.0.0" }),
      );
      await fs.writeFile(
         path.join(dir, "report.malloy"),
         `source: report is duckdb.sql("SELECT ${n} as n")\n`,
      );
   }

   it("resolves the versions a registry already holds, as after a restart", async () => {
      const registry = memoryRegistry();
      registry.rows.push(version("sales", "1.0.0"), version("sales", "1.1.0"));
      registry.latest.set("sales", "1.1.0");
      env.setVersionRegistry(registry);

      await env.loadPackageVersions();

      expect(env.isVersionedPackage("sales")).toBe(true);
      expect(env.resolveSlot("sales").version?.version).toBe("1.1.0");
      expect(env.resolveSlot("sales", "1.0.0").key).toBe("sales@1.0.0");
   });

   it("fetches a registered version's missing tree again, and serves it when it hashes to what was published", async () => {
      const source = path.join(rootDir, "source");
      await writeTree(source, 1);
      const registry = memoryRegistry();
      registry.rows.push(
         version("sales", "1.0.0", {
            sourceLocation: source,
            contentHash: await hashPackageTree(source),
         }),
      );
      registry.latest.set("sales", "1.0.0");
      env.setVersionRegistry(registry, async (location, target) => {
         await fs.cp(location, target, { recursive: true });
      });
      await env.loadPackageVersions();

      const pkg = await env.getPackage("sales");
      expect(pkg.getVersionId()).toBe("1.0.0");
      expect(pkg.getPackagePath()).toBe(
         path.join(path.resolve(envPath), "sales", "1.0.0"),
      );
   });

   it("refuses to serve a re-fetched tree whose content changed since publish", async () => {
      const source = path.join(rootDir, "source");
      await writeTree(source, 1);
      const registry = memoryRegistry();
      registry.rows.push(
         version("sales", "1.0.0", {
            sourceLocation: source,
            contentHash: "0".repeat(64),
         }),
      );
      registry.latest.set("sales", "1.0.0");
      env.setVersionRegistry(registry, async (location, target) => {
         await fs.cp(location, target, { recursive: true });
      });
      await env.loadPackageVersions();

      await expect(env.getPackage("sales")).rejects.toThrow(
         "content there has changed since it was published",
      );
      expect(
         await fs
            .stat(path.join(envPath, "sales", "1.0.0"))
            .then(() => true)
            .catch(() => false),
      ).toBe(false);
   });
});

describe("Environment versions under concurrency and failure", () => {
   let rootDir: string;
   let envPath: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-vrace-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   /** A package tree whose publisher.json names `v` and whose model answers `n`. */
   async function writePackage(dir: string, v: string, n: number) {
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "sales", version: v }),
      );
      await fs.writeFile(
         path.join(dir, "report.malloy"),
         `source: report is duckdb.sql("SELECT ${n} as n")\n`,
      );
   }

   const publish = (v: string, n: number) =>
      env.publishPackageVersion(
         "sales",
         (staging) => writePackage(staging, v, n),
         { sourceLocation: `/src/sales-${v}`, promotion: "on-publish" },
      );

   /** Every version loaded, as /status reports them. */
   const loadedVersions = async () =>
      (await env.listPackages({ everyLoadedVersion: true }))
         .map((p) => p.versionId)
         .filter((v): v is string => typeof v === "string")
         .sort();

   const tick = () => new Promise((r) => setTimeout(r, 25));

   it("serves a read that races a publish from the publish's own instance, never a second copy", async () => {
      env.setVersionRegistry(memoryRegistry());
      await publish("1.0.0", 1);

      // Hold the publish of 1.1.0 at the step after its version is in the
      // index (it is latest by then) and before it is in the package map.
      let entered: () => void = () => {};
      const reached = new Promise<void>((resolve) => (entered = resolve));
      let release: () => void = () => {};
      const gate = new Promise<void>((resolve) => (release = resolve));
      env.setStorageBindingResolver(async () => {
         entered();
         await gate;
         return {};
      });
      const publishing = publish("1.1.0", 2);
      await reached;
      env.setStorageBindingResolver(async () => ({}));
      const read = env.getPackage("sales");
      await tick();
      release();

      const [published, readBack] = await Promise.all([publishing, read]);
      expect(readBack.getVersionId()).toBe("1.1.0");
      expect(readBack).toBe(published);
   });

   it("leaves nothing loaded when a read names a version while its package is being deleted", async () => {
      const source = path.join(rootDir, "source");
      await writePackage(source, "1.0.0", 1);
      const registry = memoryRegistry();
      registry.rows.push(
         version("sales", "1.0.0", {
            sourceLocation: source,
            contentHash: await hashPackageTree(source),
         }),
         version("sales", "1.1.0"),
      );
      registry.latest.set("sales", "1.1.0");
      // 1.0.0's tree is missing, so loading it fetches it: holding the fetch
      // keeps that load, and its version lock, in flight.
      let release: () => void = () => {};
      const gate = new Promise<void>((resolve) => (release = resolve));
      let fetching: () => void = () => {};
      const started = new Promise<void>((resolve) => (fetching = resolve));
      env.setVersionRegistry(registry, async (location, target) => {
         fetching();
         await gate;
         await fs.cp(location, target, { recursive: true });
      });
      await env.loadPackageVersions();

      const inFlight = env.getPackage("sales", false, { versionId: "1.0.0" });
      await started;
      const deleting = env.deletePackage("sales");
      await tick();
      // The delete has forgotten the package's versions by now, so a read
      // that names one is refused rather than queued behind the delete.
      await expect(
         env.getPackage("sales", false, { versionId: "1.0.0" }),
      ).rejects.toMatchObject({ reason: "VERSION_NOT_FOUND" });
      release();
      await inFlight.catch(() => undefined);
      await deleting;

      expect(env.isVersionedPackage("sales")).toBe(false);
      expect(await loadedVersions()).toEqual([]);
   });

   it("keeps a version that committed when moving latest to it fails, and serves it by name", async () => {
      const registry = memoryRegistry();
      registry.setLatest = async () => {
         throw new Error("database unavailable");
      };
      env.setVersionRegistry(registry);

      const pkg = await publish("1.0.0", 1);

      expect(pkg.getVersionId()).toBe("1.0.0");
      expect(registry.rows.map((r) => r.version)).toEqual(["1.0.0"]);
      expect(env.resolveSlot("sales", "1.0.0").version?.version).toBe("1.0.0");
      // Latest never moved, so a nameless read has nothing to answer from.
      expect(refusal(() => env.resolveSlot("sales"))).toEqual({
         status: 404,
         reason: "VERSION_NOT_FOUND",
      });
   });

   it("gives a tie of equal versions to the later publish, and a re-publish never takes latest back", async () => {
      env.setVersionRegistry(memoryRegistry());
      await publish("2.0.0+b1", 1);
      await publish("2.0.0+b2", 2);
      expect(env.listPackageVersions("sales").latest).toBe("2.0.0+b2");

      // The same b1 content again is placement: latest stays on b2.
      await publish("2.0.0+b1", 1);
      expect(env.listPackageVersions("sales").latest).toBe("2.0.0+b2");
   });

   it("refuses a version that differs from a published one only by letter case", async () => {
      env.setVersionRegistry(memoryRegistry());
      await publish("1.0.0-RC.1", 1);
      await expect(publish("1.0.0-rc.1", 2)).rejects.toMatchObject({
         reason: "VERSION_CONFLICT",
      });
      expect(
         env.listPackageVersions("sales").versions.map((v) => v.version),
      ).toEqual(["1.0.0-RC.1"]);
   });

   it("keeps every published version serving when a new one fails to load", async () => {
      env.setVersionRegistry(memoryRegistry());
      await publish("1.0.0", 1);
      await expect(
         env.publishPackageVersion(
            "sales",
            async (staging) => {
               await writePackage(staging, "1.1.0", 2);
               await fs.writeFile(
                  path.join(staging, "report.malloy"),
                  "source: report is nonsense(\n",
               );
            },
            { sourceLocation: "/src/broken", promotion: "on-publish" },
         ),
      ).rejects.toThrow();
      expect(env.listPackageVersions("sales").latest).toBe("1.0.0");
      expect(
         env.listPackageVersions("sales").versions.map((v) => v.version),
      ).toEqual(["1.0.0"]);
      expect((await env.getPackage("sales")).getVersionId()).toBe("1.0.0");
      expect(
         await fs
            .stat(path.join(envPath, "sales", "1.1.0"))
            .then(() => true)
            .catch(() => false),
      ).toBe(false);
   });

   it("puts an unversioned package back exactly as it was when its first versioned publish fails", async () => {
      await writePackage(path.join(envPath, "sales"), "0.0.1", 10);
      await env.addPackage("sales");
      env.setVersionRegistry(memoryRegistry());

      await expect(
         env.publishPackageVersion(
            "sales",
            async (staging) => {
               await writePackage(staging, "1.0.0", 11);
               await fs.writeFile(
                  path.join(staging, "report.malloy"),
                  "source: report is nonsense(\n",
               );
            },
            { sourceLocation: "/src/broken", promotion: "on-publish" },
         ),
      ).rejects.toThrow();
      expect(env.isVersionedPackage("sales")).toBe(false);
      expect(
         await fs.readFile(
            path.join(envPath, "sales", "report.malloy"),
            "utf8",
         ),
      ).toContain("SELECT 10");
      expect((await env.getPackage("sales")).getVersionId()).toBeUndefined();
   });

   it("refuses to publish a version into a package watch mode mounts in place", async () => {
      const source = path.join(rootDir, "watched-source");
      await writePackage(source, "0.0.1", 1);
      await fs.symlink(source, path.join(envPath, "sales"));
      env.setVersionRegistry(memoryRegistry());

      await expect(publish("1.0.0", 1)).rejects.toThrow("watch mode");
      expect(env.isVersionedPackage("sales")).toBe(false);
      expect(
         (await fs.lstat(path.join(envPath, "sales"))).isSymbolicLink(),
      ).toBe(true);
   });
});
