// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Swap `StorageManager` for a stub in one spec file, without leaking it into
 * the rest of the run.
 *
 * Bun's `mock.module` is process-wide and cannot be undone, so a spec that
 * replaced StorageManager outright handed every later spec the stub (which is
 * why ThemeStore's specs had to live under tests/). Here the module is mocked
 * once, with a subclass of the real class whose constructor returns the
 * stub only while a spec has one installed; `stubStorageManager` removes it
 * in that file's `afterAll`, and every other `new StorageManager(...)` is the
 * real thing.
 *
 * Call it at the top level of the spec, then import the code under test
 * dynamically (`await import(...)`) so it binds to the mocked module.
 */

import { afterAll, mock } from "bun:test";
import * as storageModule from "../storage/StorageManager";

export interface StorageManagerStub {
   initialize?: (reInit?: boolean) => Promise<void>;
   getRepository?: () => Record<string, unknown>;
   getDuckDbConnection?: () => unknown;
}

const RealStorageManager = storageModule.StorageManager;
let active: StorageManagerStub | undefined;
let installed = false;

class SwitchableStorageManager extends RealStorageManager {
   constructor(...args: ConstructorParameters<typeof RealStorageManager>) {
      if (active) {
         // A derived constructor may return an object instead of `this`.
         return {
            initialize: async () => {},
            ...active,
         } as unknown as SwitchableStorageManager;
      }
      super(...args);
   }
}

/** Install `stub` as every StorageManager built until this file's afterAll. */
export function stubStorageManager(stub: StorageManagerStub): void {
   active = stub;
   if (!installed) {
      installed = true;
      mock.module("../storage/StorageManager", () => ({
         ...storageModule,
         StorageManager: SwitchableStorageManager,
      }));
   }
   afterAll(() => {
      if (active === stub) active = undefined;
   });
}

/**
 * A repository with nothing stored: lists are empty, lookups miss, and
 * creates echo what they were given. `overrides` replaces any method.
 */
export function emptyRepository(
   overrides: Record<string, unknown> = {},
): Record<string, unknown> {
   return {
      listEnvironments: async () => [],
      getEnvironmentByName: async () => null,
      createEnvironment: async (data: Record<string, unknown>) => ({
         id: "env-id",
         name: data.name,
         path: data.path,
      }),
      listPackages: async () => [],
      getPackageByName: async () => null,
      createPackage: async (data: Record<string, unknown>) => ({
         id: "pkg-id",
         name: data.name,
      }),
      listConnections: async () => [],
      listStorageDestinations: async () => [],
      ...overrides,
   };
}
