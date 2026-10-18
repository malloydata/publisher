// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach } from "bun:test";
import * as sinon from "sinon";
import { ConnectionNotFoundError, DestinationNotFoundError } from "../errors";
import type { ResourceRepository } from "../storage/DatabaseInterface";
import type { EnvironmentStore } from "../service/environment_store";
import { MaterializationService } from "../service/materialization_service";

/**
 * Shared harness for the MaterializationService unit specs: a sinon-faked
 * EnvironmentStore whose repository is a set of stubs, plus the environment
 * and package stand-ins the build path reads. Every spec that drives the
 * service (lifecycle, instructed runs, self-instructions, incremental refresh,
 * storage builds, drop/cleanup) builds on the same fakes, so they live once.
 */

export type MockRepo = sinon.SinonStubbedInstance<ResourceRepository>;

export interface ServiceMocks {
   sandbox: sinon.SinonSandbox;
   repository: MockRepo;
   environmentStore: EnvironmentStore;
   service: MaterializationService;
}

/**
 * Turn per-query metadata on for one test and put the environment back after.
 * The feature ships dark, so a test that asserts what a statement carries has
 * to enable it — and saying so per test keeps the default honest here: a test
 * that forgets is testing the shipped default, which is "nothing attached".
 */
export function withQueryMetadataOn(): void {
   const original = process.env.PUBLISHER_QUERY_METADATA;
   beforeEach(() => {
      process.env.PUBLISHER_QUERY_METADATA = "on";
   });
   afterEach(() => {
      if (original === undefined) delete process.env.PUBLISHER_QUERY_METADATA;
      else process.env.PUBLISHER_QUERY_METADATA = original;
   });
}

/**
 * A MaterializationService over a faked EnvironmentStore. By default the
 * environment resolves and yields a package whose build plan is empty (no
 * persist sources); individual tests override getBuildPlan via `setPackage`
 * to exercise the orchestrated path. getModelPaths()=>[] keeps the background
 * build from touching the Malloy runtime.
 */
export function createMocks(): ServiceMocks {
   const sandbox = sinon.createSandbox();

   const repository = {
      getEnvironmentByName: sandbox.stub(),
      listMaterializations: sandbox.stub(),
      getMaterializationById: sandbox.stub(),
      getActiveMaterialization: sandbox.stub(),
      createMaterialization: sandbox.stub(),
      updateMaterialization: sandbox.stub(),
      deleteMaterialization: sandbox.stub(),
   } as unknown as MockRepo;

   const storageManager = { getRepository: () => repository };

   const environmentStore = {
      storageManager,
      getEnvironment: sandbox.stub(),
   } as unknown as EnvironmentStore;

   repository.getEnvironmentByName.resolves({
      id: "env-1",
      name: "my-env",
      path: "/test",
      createdAt: new Date(),
      updatedAt: new Date(),
   });
   setPackage(environmentStore, { getBuildPlan: () => null });

   const service = new MaterializationService(environmentStore);

   return { sandbox, repository, environmentStore, service };
}

/**
 * A stub of the environment surface the build path depends on, with the two
 * lookups kept as disjoint as the real Environment keeps them: connection names
 * resolve only through `getApiConnection` and destination names only through
 * `getStorageDestination`, and asking either for the other's name fails
 * the way the real one does.
 *
 * That disjointness is the point. A stub that answered both lookups from one map
 * would let a build resolve its destination through the connection list and the
 * test would never notice, which is exactly the confusion these tests exist to
 * catch.
 */
export function buildEnvironmentStub(opts: {
   connections?: Record<string, Record<string, unknown>>;
   destinations?: Record<string, Record<string, unknown>>;
   environmentPath?: string;
}) {
   const connections = opts.connections ?? {};
   const destinations = opts.destinations ?? {};
   return {
      getApiConnection: sinon.stub().callsFake((name: string) => {
         const connection = connections[name];
         if (!connection) {
            throw new ConnectionNotFoundError(`Connection ${name} not found`);
         }
         return connection;
      }),
      getStorageDestination: sinon.stub().callsFake((name: string) => {
         const destination = destinations[name];
         if (!destination) {
            throw new DestinationNotFoundError(
               `Storage destination ${name} not found`,
            );
         }
         return destination;
      }),
      getEnvironmentPath: () => opts.environmentPath ?? "/tmp/env",
   };
}

/** Point environmentStore.getEnvironment at a package with the given overrides. */
export function setPackage(
   environmentStore: EnvironmentStore,
   pkgOverrides: Record<string, unknown>,
): void {
   const pkg = {
      getBuildPlan: () => null,
      getModelPaths: () => [],
      getPackagePath: () => "/test",
      getMalloyConfig: () => ({}),
      getMalloyConnection: async () => ({}),
      // No bound manifestLocation by default, so seedFromBoundManifest no-ops.
      getPackageMetadata: () => ({}),
      ...pkgOverrides,
   };
   (environmentStore.getEnvironment as sinon.SinonStub).resolves({
      getPackage: sinon.stub().resolves(pkg),
      withPackageLock: async (_name: string, fn: () => Promise<unknown>) =>
         fn(),
      reloadAllModelsForPackage: sinon.stub().resolves(),
   });
}
