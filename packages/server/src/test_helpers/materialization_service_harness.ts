// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach } from "bun:test";
import * as sinon from "sinon";
import { ConnectionNotFoundError, DestinationNotFoundError } from "../errors";
import type { ResourceRepository } from "../storage/DatabaseInterface";
import type { Environment } from "../service/environment";
import type { EnvironmentStore } from "../service/environment_store";
import {
   MaterializationService,
   type RunCollaboratorOverrides,
} from "../service/materialization/run/materialization_service";
import { installSettings } from "./settings_env";

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
 * Turn per-query metadata on for one test and put the settings back after.
 * The feature ships dark, so a test that asserts what a statement carries has
 * to enable it — and saying so per test keeps the default honest here: a test
 * that forgets is testing the shipped default, which is "nothing attached".
 * Installing settings replaces them all, so every other setting is at its
 * default for these tests.
 */
export function withQueryMetadataOn(): void {
   let restore: (() => void) | undefined;
   beforeEach(() => {
      restore = installSettings({
         env: { PUBLISHER_QUERY_METADATA: "on" },
      }).restore;
   });
   afterEach(() => {
      restore?.();
      restore = undefined;
   });
}

/**
 * A MaterializationService over a faked EnvironmentStore. By default the
 * environment resolves and yields a package whose build plan is empty (no
 * persist sources); individual tests override getBuildPlan via `setPackage`
 * to exercise the orchestrated path. getModelPaths()=>[] keeps the background
 * build from touching the Malloy runtime. `collaborators` replaces the steps a
 * run takes (see RunCollaborators); a spec that replaces one after the mocks
 * exist constructs another service over `environmentStore` with them.
 */
export function createMocks(
   collaborators: RunCollaboratorOverrides = {},
): ServiceMocks {
   const sandbox = sinon.createSandbox();

   const repository = {
      getEnvironmentByName: sandbox.stub(),
      listMaterializations: sandbox.stub(),
      getMaterializationById: sandbox.stub(),
      getActiveMaterialization: sandbox.stub(),
      createMaterialization: sandbox.stub(),
      updateMaterialization: sandbox.stub(),
      updateMaterializationFrom: sandbox.stub(),
      deleteMaterialization: sandbox.stub(),
   } as unknown as MockRepo;
   // The conditional write, as the store makes it, over the status the read
   // stub answers (null: the run is gone), through the write stub a spec
   // configures. A read stub that answers nothing, or throws, lets it land.
   repository.updateMaterializationFrom.callsFake(
      async (id, updates, fromStatuses) => {
         const current = await Promise.resolve(
            repository.getMaterializationById(id),
         ).catch(() => undefined);
         if (
            current === null ||
            (current && !fromStatuses.includes(current.status))
         ) {
            return null;
         }
         return repository.updateMaterialization(id, updates);
      },
   );

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

   const service = new MaterializationService(environmentStore, collaborators);

   return { sandbox, repository, environmentStore, service };
}

/**
 * Another service over `mocks`' faked store, with `collaborators` replacing
 * the steps a run takes.
 */
export function serviceWith(
   mocks: Pick<ServiceMocks, "environmentStore">,
   collaborators: RunCollaboratorOverrides,
): MaterializationService {
   return new MaterializationService(mocks.environmentStore, collaborators);
}

/**
 * A MaterializationService over a real repository and environment, the way the
 * server wires one, with the store answering every environment name with that
 * environment. Both are read through thunks, so a spec that reopens either
 * keeps the one service.
 */
export function serviceOverEnvironment(
   repository: () => ResourceRepository,
   environment: () => Environment,
   collaborators: RunCollaboratorOverrides = {},
): MaterializationService {
   const store = {
      storageManager: { getRepository: () => repository() },
      getEnvironment: async () => environment(),
      getLoadedEnvironments: () => [environment()],
   } as unknown as EnvironmentStore;
   return new MaterializationService(store, collaborators);
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
      getEnvironmentName: () => "env",
      ...UNVERSIONED_ENVIRONMENT,
   };
}

/**
 * The version surface of an environment whose packages have no published
 * versions. Every fake environment the materialization specs build spreads it
 * in.
 */
export const UNVERSIONED_ENVIRONMENT = {
   getVersionService: () => null,
   getLoadedVersionIds: () => [],
   peekVersion: () => undefined,
   isHostOwned: (pkg: {
      getPackageMetadata(): { manifestLocation?: string | null };
   }) => Boolean(pkg.getPackageMetadata().manifestLocation),
   markVersionHostOwned: async () => undefined,
   registerVersionBuild: () => () => undefined,
   // Nothing configured is refused, and nothing is stale.
   publishRefusedConfiguredPackage: async () => undefined,
   getRefusedReloads: () => new Map<string, unknown>(),
   // Every package a spec names is loaded.
   hasLoadedPackage: () => true,
};

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
      getVersionId: () => undefined,
      getMaterializationConfig: () => null,
      whenBuildPlanCurrent: async () => undefined,
      isBuildPlanUnavailable: () => false,
      // No served copy, so a chained build recomputes its upstreams.
      chainBuildText: () => ({
         declined: "the test double has no served copy",
      }),
      // Binds no key check, so there is none to forget.
      forgetKeyUniqueness: () => undefined,
      ...pkgOverrides,
   };
   (environmentStore.getEnvironment as sinon.SinonStub).resolves({
      ...UNVERSIONED_ENVIRONMENT,
      getPackage: sinon.stub().resolves(pkg),
      withPackageLock: async (_name: string, fn: () => Promise<unknown>) =>
         fn(),
      reloadAllModelsForPackage: sinon.stub().resolves(),
   });
}
