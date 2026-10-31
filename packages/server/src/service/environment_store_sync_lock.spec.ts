// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Syncing one package's row (`addPackageToDatabase`, after a POST with no
 * location or an unversioned PATCH) must not write over the row of a package
 * that a first versioned publish took over while the sync was deciding. The
 * real method runs over a fake environment whose package lock is a real one.
 */
import { describe, expect, it } from "bun:test";
import { EnvironmentStore } from "./environment_store";

/** One lock per package name, as Environment.withPackageLock serializes. */
function packageLocks() {
   const tails = new Map<string, Promise<unknown>>();
   return <T>(name: string, fn: () => Promise<T>): Promise<T> => {
      const run = (tails.get(name) ?? Promise.resolve()).then(fn, fn);
      tails.set(
         name,
         run.catch(() => undefined),
      );
      return run;
   };
}

describe("addPackageToDatabase", () => {
   it("does not write over a package that a first versioned publish took over meanwhile", async () => {
      let published = false;
      const withPackageLock = packageLocks();
      const environment = {
         listPackages: async () => [{ name: "sales", description: "old tree" }],
         getVersionService: () => ({
            isVersioned: async () => published,
         }),
         withPackageLock,
      };
      const writes: unknown[] = [];
      const store = {
         getEnvironment: async () => environment,
         storageManager: {
            getRepository: () => ({
               getEnvironmentByName: async () => ({ id: "env-1" }),
            }),
         },
         isVersionedPackage: (
            EnvironmentStore.prototype as unknown as {
               isVersionedPackage: unknown;
            }
         ).isVersionedPackage,
         addPackage: async (pkg: unknown) => {
            writes.push(pkg);
         },
      };

      // A first versioned publish holds the package lock until it commits.
      let commit!: () => void;
      const committed = new Promise<void>((resolve) => (commit = resolve));
      const publish = withPackageLock("sales", async () => {
         await committed;
         published = true;
      });
      const sync = EnvironmentStore.prototype.addPackageToDatabase.call(
         store as unknown as EnvironmentStore,
         "env",
         "sales",
      );
      // Let the sync reach its check before the publish commits.
      await new Promise((resolve) => setTimeout(resolve, 10));
      commit();
      await Promise.all([publish, sync]);

      expect(writes).toEqual([]);
   });

   it("writes the row of a package with no versions", async () => {
      const environment = {
         listPackages: async () => [{ name: "sales", description: "tree" }],
         getVersionService: () => ({ isVersioned: async () => false }),
         withPackageLock: packageLocks(),
      };
      const writes: unknown[] = [];
      const store = {
         getEnvironment: async () => environment,
         storageManager: {
            getRepository: () => ({
               getEnvironmentByName: async () => ({ id: "env-1" }),
            }),
         },
         isVersionedPackage: (
            EnvironmentStore.prototype as unknown as {
               isVersionedPackage: unknown;
            }
         ).isVersionedPackage,
         addPackage: async (pkg: unknown) => {
            writes.push(pkg);
         },
      };
      await EnvironmentStore.prototype.addPackageToDatabase.call(
         store as unknown as EnvironmentStore,
         "env",
         "sales",
      );
      expect(writes).toEqual([{ name: "sales", description: "tree" }]);
   });
});
