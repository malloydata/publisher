// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A stand-in `StorageManager` to inject through
 * `new EnvironmentStore(root, { storageManager })`. `initialize` defaults to a
 * no-op; anything the stub omits is absent, so an unexpected call fails loudly.
 */

import type { StorageManager } from "../storage/StorageManager";

export interface StorageManagerStub {
   initialize?: (reInit?: boolean) => Promise<void>;
   getRepository?: () => Record<string, unknown>;
   getDuckDbConnection?: () => unknown;
}

export function stubStorageManager(stub: StorageManagerStub): StorageManager {
   return {
      initialize: async () => {},
      ...stub,
   } as unknown as StorageManager;
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
      // The version registry: no package here has published versions.
      listVersions: async () => [],
      listVersionsByEnvironment: async () => [],
      hasVersions: async () => false,
      getVersion: async () => null,
      ...overrides,
   };
}
