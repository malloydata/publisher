import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   buildVersionedPackagePath,
   PackageSweeper,
   VERSIONED_PACKAGE_DIR_SUFFIX,
} from "./package_sweeper";

interface FakeRepository {
   getEnvironmentByName(name: string): Promise<{ id: string } | null>;
   listPackageDirectoryPaths(environmentId: string): Promise<Set<string>>;
}

interface FakeEnvironment {
   getEnvironmentName(): string;
   getEnvironmentPath(): string;
   evictPackageAtPath(path: string): void;
   evictedPaths: string[];
}

function buildEnv(name: string, envPath: string): FakeEnvironment {
   const evicted: string[] = [];
   return {
      getEnvironmentName: () => name,
      getEnvironmentPath: () => envPath,
      evictPackageAtPath: (p: string) => evicted.push(p),
      evictedPaths: evicted,
   };
}

function buildStore(
   environments: FakeEnvironment[],
   referenced: Map<string, Set<string>>,
) {
   return {
      listInMemoryEnvironments: () => environments,
      getEnvironment: async (name: string) =>
         environments.find((e) => e.getEnvironmentName() === name) ??
         (() => {
            throw new Error(`unknown env ${name}`);
         })(),
      storageManager: {
         getRepository: (): FakeRepository => ({
            getEnvironmentByName: async (name: string) => ({
               id: `id-${name}`,
            }),
            listPackageDirectoryPaths: async (environmentId: string) => {
               return referenced.get(environmentId) ?? new Set<string>();
            },
         }),
      },
   };
}

function ageMs(): number {
   return 60 * 1000;
}

describe("PackageSweeper", () => {
   let tmpRoot: string;

   beforeEach(async () => {
      tmpRoot = await fs.promises.mkdtemp(
         path.join(os.tmpdir(), "pkg-sweeper-"),
      );
   });

   afterEach(async () => {
      await fs.promises.rm(tmpRoot, { recursive: true, force: true });
   });

   it("removes orphaned versioned directories that age past quiescence", async () => {
      const envName = "env-a";
      const envPath = path.join(tmpRoot, envName);
      await fs.promises.mkdir(envPath, { recursive: true });

      const live = buildVersionedPackagePath(envPath, "pkg-a", "v-live");
      const orphan = buildVersionedPackagePath(envPath, "pkg-a", "v-orphan");
      await fs.promises.mkdir(live, { recursive: true });
      await fs.promises.mkdir(orphan, { recursive: true });

      // Backdate both directories so they are older than the quiescence window.
      const old = new Date(Date.now() - 10 * 60_000);
      await fs.promises.utimes(live, old, old);
      await fs.promises.utimes(orphan, old, old);

      const env = buildEnv(envName, envPath);
      // v-live is referenced by the database; v-orphan is not.
      const referenced = new Map([[`id-${envName}`, new Set([live])]]);
      const store = buildStore([env], referenced) as never;

      const sweeper = new PackageSweeper(store, {
         quiescenceMs: ageMs(),
         periodicIntervalMs: 60_000,
      });
      await sweeper.sweepAllEnvironments();

      expect(fs.existsSync(live)).toBe(true);
      expect(fs.existsSync(orphan)).toBe(false);
      expect(env.evictedPaths).toEqual([orphan]);
   });

   it("preserves orphans that are still inside the quiescence window", async () => {
      const envName = "env-b";
      const envPath = path.join(tmpRoot, envName);
      const orphan = buildVersionedPackagePath(envPath, "pkg-b", "fresh");
      await fs.promises.mkdir(orphan, { recursive: true });

      const env = buildEnv(envName, envPath);
      // Empty referenced set — every dir is unreferenced.
      const store = buildStore([env], new Map()) as never;

      const sweeper = new PackageSweeper(store, {
         quiescenceMs: 60 * 60_000, // 1 hour, won't elapse
         periodicIntervalMs: 60_000,
      });
      await sweeper.sweepAllEnvironments();

      expect(fs.existsSync(orphan)).toBe(true);
      expect(env.evictedPaths).toEqual([]);
   });

   it("never touches paths outside an *.versions/ directory", async () => {
      const envName = "env-c";
      const envPath = path.join(tmpRoot, envName);
      await fs.promises.mkdir(envPath, { recursive: true });

      // A bare `<env>/<pkg>/` directory that mimics a legacy or
      // mount-style package; the sweeper must leave it alone even when
      // the DB does not reference it.
      const legacyMount = path.join(envPath, "mount-style-pkg");
      await fs.promises.mkdir(legacyMount, { recursive: true });

      const env = buildEnv(envName, envPath);
      const store = buildStore([env], new Map()) as never;
      const sweeper = new PackageSweeper(store, {
         quiescenceMs: ageMs(),
         periodicIntervalMs: 60_000,
      });
      await sweeper.sweepAllEnvironments();

      expect(fs.existsSync(legacyMount)).toBe(true);
   });

   it("scheduleSweep refuses paths that are not under an *.versions/ parent", async () => {
      const envName = "env-d";
      const envPath = path.join(tmpRoot, envName);
      await fs.promises.mkdir(envPath, { recursive: true });
      const externallyMounted = path.join(envPath, "absolute-mount");
      await fs.promises.mkdir(externallyMounted, { recursive: true });

      const env = buildEnv(envName, envPath);
      const store = buildStore([env], new Map()) as never;

      // Custom sweeper with a tiny quiescence so the scheduled timer fires.
      const sweeper = new PackageSweeper(store, {
         quiescenceMs: 1,
         periodicIntervalMs: 60_000,
      });
      sweeper.scheduleSweep(envName, externallyMounted);
      // Allow any pending timers to fire.
      await new Promise((r) => setTimeout(r, 50));
      expect(fs.existsSync(externallyMounted)).toBe(true);

      sweeper.stop();
   });

   it("scheduleSweep deletes a versioned directory once the quiescence window elapses", async () => {
      const envName = "env-e";
      const envPath = path.join(tmpRoot, envName);
      const orphan = buildVersionedPackagePath(envPath, "pkg", "to-sweep");
      await fs.promises.mkdir(orphan, { recursive: true });

      const env = buildEnv(envName, envPath);
      const store = buildStore([env], new Map()) as never;

      const quiescenceMs = 50;
      const sweeper = new PackageSweeper(store, {
         quiescenceMs,
         periodicIntervalMs: 60_000,
      });
      // Backdate the directory so it is already older than the quiescence
      // window when the timer fires.
      const old = new Date(Date.now() - 60_000);
      await fs.promises.utimes(orphan, old, old);

      sweeper.scheduleSweep(envName, orphan);

      // Wait for the scheduled timer to fire and the rm to complete.
      await new Promise((r) => setTimeout(r, quiescenceMs + 100));

      expect(fs.existsSync(orphan)).toBe(false);
      expect(env.evictedPaths).toEqual([orphan]);
      sweeper.stop();
   });

   it("buildVersionedPackagePath co-locates every version of a package", () => {
      const a = buildVersionedPackagePath("/env", "pkg", "v1");
      const b = buildVersionedPackagePath("/env", "pkg", "v2");
      expect(path.dirname(a)).toBe(path.dirname(b));
      expect(path.basename(path.dirname(a))).toBe(
         `pkg${VERSIONED_PACKAGE_DIR_SUFFIX}`,
      );
   });
});
