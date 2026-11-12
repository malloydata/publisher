// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it, spyOn } from "bun:test";
import sinon from "sinon";

import type { components } from "../api";
import {
   BadRequestError,
   FrozenConfigError,
   PackageAdmissionRefusedError,
   PackageVersionError,
   BuildPlanRefreshingError,
   BuildPlanUnavailableError,
   internalErrorToHttpError,
   ServiceUnavailableError,
   InstalledFromLocationError,
} from "../errors";
import { logger } from "../logger";
import { installSettings } from "../test_helpers/settings_env";
import { PackageController } from "./package.controller";
import { fakeOf } from "../test_helpers/store_fakes";
import type { PackageAdmin } from "../service/environment_ports";
import { testRetrieval } from "../test_helpers/test_retrieval";

let restoreSettings: (() => void) | undefined;

/** Run this test with `PUBLISHER_PACKAGE_VERSIONING` at `value`, or unset. */
function versioning(value?: string): void {
   restoreSettings?.();
   restoreSettings = installSettings({
      env: value === undefined ? {} : { PUBLISHER_PACKAGE_VERSIONING: value },
   }).restore;
}

/** Turn package versioning on for this test, as a versioned publish needs. */
function versioningOn(): void {
   versioning("on");
}

afterEach(() => {
   restoreSettings?.();
   restoreSettings = undefined;
});

// DEPRECATED FEATURE COVERAGE — delete together with `explores` (see RELEASE_NOTES deprecations).
describe("deprecated: explores — PackageController.addPackage validation", () => {
   afterEach(() => {
      sinon.restore();
   });

   it("no-location: rejects invalid explores and rolls back via unloadPackage (NOT deletePackage)", async () => {
      // The no-location path registers a PRE-EXISTING user directory, so a bad
      // manifest must unload it from memory — never deletePackage, which would
      // delete the user's files.
      const invalidMsg =
         "Invalid explores entry 'missing.malloy' in publisher.json: file not found";
      const mockPackage = {
         formatInvalidExplores: () => invalidMsg,
         formatInvalidPersistencePolicy: () => "",
         formatUnparseableAnnotations: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const unloadPackage = sinon.stub().resolves(undefined);
      const deletePackage = sinon.stub().resolves(undefined);
      const addPackage = sinon.stub().resolves(mockPackage);
      const environment = {
         getVersionService: () => null,
         addPackage,
         unloadPackage,
         deletePackage,
      };
      const getEnvironment = sinon.stub().resolves(environment);
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      });

      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );

      await expect(
         controller.addPackage("env", {
            name: "pkg",
            description: "test",
            explores: ["missing.malloy"],
         }),
      ).rejects.toBeInstanceOf(BadRequestError);

      expect(unloadPackage.calledOnceWith("pkg")).toBe(true);
      expect(deletePackage.called).toBe(false);
      expect(addPackageToDatabase.called).toBe(false);
   });

   it("location: a published version runs the publish checks before it is recorded, not as a controller delete", async () => {
      // A publish from a location is a published version: its tree was just
      // downloaded, and the version service runs the checks before anything
      // is recorded, so a refusal leaves nothing to delete or unload.
      versioningOn();
      const invalidMsg =
         "Invalid explores entry 'missing.malloy' in publisher.json: file not found";
      const mockPackage = {
         formatInvalidExplores: () => invalidMsg,
         formatInvalidPersistencePolicy: () => "",
         formatUnparseableAnnotations: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const versions = versionsPublishing(mockPackage);
      const unloadPackage = sinon.stub().resolves(undefined);
      const deletePackage = sinon.stub().resolves(undefined);
      const environment = {
         getVersionService: () => versions,
         unloadPackage,
         deletePackage,
      };
      const getEnvironment = sinon.stub().resolves(environment);
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      });

      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );

      await expect(
         controller.addPackage("env", {
            name: "pkg",
            description: "test",
            location: "gs://bucket/pkg.zip",
            explores: ["missing.malloy"],
         }),
      ).rejects.toBeInstanceOf(BadRequestError);

      expect(versions.publish.calledOnce).toBe(true);
      expect(typeof versions.publish.firstCall.args[2].validate).toBe(
         "function",
      );
      expect(deletePackage.called).toBe(false);
      expect(unloadPackage.called).toBe(false);
      expect(addPackageToDatabase.called).toBe(false);
   });

   it("location: validation runs inside installPackage's rollback window, not as a controller delete", async () => {
      // For the location path the tree was freshly downloaded, so validation is
      // delegated to installPackage (which rolls the swap back on failure). The
      // controller passes a validator and does NOT call delete/unload itself.
      const invalidMsg =
         "Invalid explores entry 'missing.malloy' in publisher.json: file not found";
      const mockPackage = {
         formatInvalidExplores: () => invalidMsg,
         formatInvalidPersistencePolicy: () => "",
         formatUnparseableAnnotations: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      // installPackage mimics the real contract: invoke the validator and, if it
      // returns a message, throw BadRequestError (after its internal rollback).
      const installPackage = sinon
         .stub()
         .callsFake(
            async (
               _name: string,
               _downloader: unknown,
               validate?: (pkg: unknown) => string | undefined,
            ) => {
               const msg = validate?.(mockPackage);
               if (msg) throw new BadRequestError(msg);
               return mockPackage;
            },
         );
      const unloadPackage = sinon.stub().resolves(undefined);
      const deletePackage = sinon.stub().resolves(undefined);
      const environment = {
         getVersionService: () => null,
         installPackage,
         unloadPackage,
         deletePackage,
      };
      const getEnvironment = sinon.stub().resolves(environment);
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      });

      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );

      await expect(
         controller.addPackage("env", {
            name: "pkg",
            description: "test",
            location: "gs://bucket/pkg.zip",
            explores: ["missing.malloy"],
         }),
      ).rejects.toBeInstanceOf(BadRequestError);

      expect(installPackage.calledOnce).toBe(true);
      expect(typeof installPackage.firstCall.args[2]).toBe("function");
      expect(deletePackage.called).toBe(false);
      expect(unloadPackage.called).toBe(false);
      expect(addPackageToDatabase.called).toBe(false);
   });

   it("persists when explores are valid (no-location)", async () => {
      const mockPackage = {
         formatInvalidExplores: () => "",
         formatInvalidPersistencePolicy: () => "",
         formatUnparseableAnnotations: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const addPackage = sinon.stub().resolves(mockPackage);
      const getEnvironment = sinon
         .stub()
         .resolves({ addPackage, getVersionService: () => null });
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      });

      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );

      await controller.addPackage("env", {
         name: "pkg",
         description: "test",
         explores: ["index.malloy"],
      });

      expect(addPackageToDatabase.calledOnceWith("env", "pkg")).toBe(true);
   });
});

describe("PackageController.addPackage persistence policy validation", () => {
   afterEach(() => {
      sinon.restore();
   });

   it("rejects a publish whose persistence policy is invalid (no-location path)", async () => {
      // An invalid persistence policy (e.g. a schedule on a package-scoped
      // package): the publish must 400 (strict-at-publish; load merely warns)
      // and roll back via
      // unloadPackage.
      const cronMsg =
         'materialization.schedule (cron) in publisher.json requires "scope": ' +
         '"version".';
      const mockPackage = {
         formatInvalidExplores: () => "",
         formatInvalidPersistencePolicy: () => cronMsg,
         formatUnparseableAnnotations: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const unloadPackage = sinon.stub().resolves(undefined);
      const addPackage = sinon.stub().resolves(mockPackage);
      const environment = {
         getVersionService: () => null,
         addPackage,
         unloadPackage,
      };
      const getEnvironment = sinon.stub().resolves(environment);
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      });

      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );

      await expect(
         controller.addPackage("env", { name: "pkg", description: "test" }),
      ).rejects.toThrow(cronMsg);

      expect(unloadPackage.calledOnceWith("pkg")).toBe(true);
      expect(addPackageToDatabase.called).toBe(false);
   });

   it("location path: the persistence-policy gate refuses the version before it is recorded", async () => {
      versioningOn();
      const cronMsg =
         'materialization.schedule (cron) in publisher.json requires "scope": ' +
         '"version".';
      const mockPackage = {
         formatInvalidExplores: () => "",
         formatInvalidPersistencePolicy: () => cronMsg,
         formatUnparseableAnnotations: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const environment = {
         getVersionService: () => versionsPublishing(mockPackage),
      };
      const getEnvironment = sinon.stub().resolves(environment);
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      });

      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );

      await expect(
         controller.addPackage("env", {
            name: "pkg",
            description: "test",
            location: "gs://bucket/pkg.zip",
         }),
      ).rejects.toThrow(cronMsg);

      expect(addPackageToDatabase.called).toBe(false);
   });

   it("location path: the persistence-policy gate runs inside installPackage's rollback window", async () => {
      const cronMsg =
         'materialization.schedule (cron) in publisher.json requires "scope": ' +
         '"version".';
      const mockPackage = {
         formatInvalidExplores: () => "",
         formatInvalidPersistencePolicy: () => cronMsg,
         formatUnparseableAnnotations: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const installPackage = sinon
         .stub()
         .callsFake(
            async (
               _name: string,
               _downloader: unknown,
               validate?: (pkg: unknown) => string | undefined,
            ) => {
               const msg = validate?.(mockPackage);
               if (msg) throw new BadRequestError(msg);
               return mockPackage;
            },
         );
      const environment = { getVersionService: () => null, installPackage };
      const getEnvironment = sinon.stub().resolves(environment);
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      });

      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );

      await expect(
         controller.addPackage("env", {
            name: "pkg",
            description: "test",
            location: "gs://bucket/pkg.zip",
         }),
      ).rejects.toThrow(cronMsg);

      expect(addPackageToDatabase.called).toBe(false);
   });
});

describe("PackageController.addPackage incremental policy validation", () => {
   afterEach(() => {
      sinon.restore();
   });

   it("joins the incremental gate into the same 400 as the other publish gates", async () => {
      // The incremental-refresh gate is the fourth strict-at-publish check. A
      // publish that trips two gates must report BOTH — the author fixes one
      // round-trip, not one message at a time.
      const cronMsg =
         'materialization.schedule (cron) in publisher.json requires "scope": ' +
         '"version".';
      const incrementalMsg =
         '#@ persist source "daily_revenue" declares refresh="incremental" but ' +
         "no watermark=.";
      const mockPackage = {
         formatInvalidExplores: () => "",
         formatInvalidPersistencePolicy: () => cronMsg,
         formatUnparseableAnnotations: () => "",
         formatInvalidIncrementalPolicy: () => incrementalMsg,
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const unloadPackage = sinon.stub().resolves(undefined);
      const addPackage = sinon.stub().resolves(mockPackage);
      const getEnvironment = sinon.stub().resolves({
         addPackage,
         unloadPackage,
         getVersionService: () => null,
      });
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      });

      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );

      const error = await controller
         .addPackage("env", { name: "pkg", description: "test" })
         .then(
            () => undefined,
            (err: unknown) => err as Error,
         );

      expect(error).toBeInstanceOf(BadRequestError);
      expect(error!.message).toBe(`${cronMsg}\n${incrementalMsg}`);
      expect(unloadPackage.calledOnceWith("pkg")).toBe(true);
      expect(addPackageToDatabase.called).toBe(false);
   });

   it("publishes when the incremental declaration is the only thing declared and it is valid", async () => {
      const mockPackage = {
         formatInvalidExplores: () => "",
         formatInvalidPersistencePolicy: () => "",
         formatUnparseableAnnotations: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const addPackage = sinon.stub().resolves(mockPackage);
      const getEnvironment = sinon
         .stub()
         .resolves({ addPackage, getVersionService: () => null });
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      });

      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );
      await controller.addPackage("env", { name: "pkg", description: "test" });

      expect(addPackageToDatabase.calledOnceWith("env", "pkg")).toBe(true);
   });
});

// DEPRECATED FEATURE COVERAGE — delete together with `explores` (see RELEASE_NOTES deprecations).
describe("deprecated: explores — PackageController.updatePackage validation", () => {
   afterEach(() => {
      sinon.restore();
   });

   it("location update: validates the EFFECTIVE explores (body override) before the swap commits", async () => {
      // body.location triggers a reinstall (atomic swap). The effective explores
      // — the body override here — must be validated inside installPackage so a
      // bad update rolls back to the previous tree instead of swapping in the
      // rejected one and 400-ing after the fact.
      const invalidMsg =
         "Invalid explores entry 'nope.malloy' in publisher.json: file not found";
      // The mock package validates whatever override it's handed.
      const mockPackage = {
         formatInvalidExplores: (override?: string[]) =>
            override?.includes("nope.malloy") ? invalidMsg : "",
         formatInvalidPersistencePolicy: () => "",
         formatUnparseableAnnotations: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const installPackage = sinon
         .stub()
         .callsFake(
            async (
               _name: string,
               _downloader: unknown,
               validate?: (pkg: unknown) => string | undefined,
            ) => {
               const msg = validate?.(mockPackage);
               if (msg) throw new BadRequestError(msg);
               return mockPackage;
            },
         );
      const updatePackage = sinon.stub().resolves(mockPackage);
      const environment = {
         getVersionService: () => null,
         peekPackage: () => undefined,
         awaitPackageLoads: async () => {},
         installPackage,
         updatePackage,
      };
      const getEnvironment = sinon.stub().resolves(environment);
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      });

      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );

      await expect(
         controller.updatePackage("env", "pkg", {
            name: "pkg",
            location: "gs://bucket/pkg.zip",
            explores: ["nope.malloy"],
         }),
      ).rejects.toBeInstanceOf(BadRequestError);

      // The rejected swap never reached the metadata-apply / persist steps.
      expect(updatePackage.called).toBe(false);
      expect(addPackageToDatabase.called).toBe(false);
   });
});

/** The schema's own type, so a stubbed status cannot drift from the real one. */
type EmbeddingIndex =
   | components["schemas"]["PackageEmbeddingIndex"]
   | undefined;

describe("PackageController.getPackage embeddingIndex", () => {
   afterEach(() => {
      sinon.restore();
   });

   /**
    * A controller whose package metadata is fixed and whose index lookup is
    * stubbed to a known status. The index lookup is not the oracle here — the
    * defect being pinned is control flow in the controller, where the
    * enrichment sat below a `reload` early return, so `?reload=true` answered
    * without the field. Stubbing it is what lets both branches be compared
    * without an embedding provider, which `getPackageEmbeddingStatus`
    * otherwise short-circuits on.
    */
   const controllerWithIndex = (embeddingIndex: EmbeddingIndex) => {
      const metadata = { name: "pkg", resource: "/pkg" };
      const _package = {
         getPackageMetadata: () => metadata,
         getOperationalFields: () => ({}),
      };
      const getPackage = sinon.stub().resolves(_package);
      const environment = {
         getVersionService: () => null,
         getPackage,
         describePackageStatus: () => ({ serving: true, loading: false }),
      };
      const getEnvironment = sinon.stub().resolves(environment);
      const environmentStore = fakeOf<PackageAdmin>({
         getEnvironment,
      });
      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );
      sinon
         .stub(
            controller as unknown as {
               embeddingIndexStatus: () => Promise<unknown>;
            },
            "embeddingIndexStatus",
         )
         .resolves(embeddingIndex);
      return { controller, getPackage };
   };

   it("reports the index on a plain GET and on a reload alike", async () => {
      const status: EmbeddingIndex = { status: "indexing" };

      const plain = controllerWithIndex(status);
      const withoutReload = await plain.controller.getPackage(
         "env",
         "pkg",
         false,
      );

      const reloaded = controllerWithIndex(status);
      const withReload = await reloaded.controller.getPackage(
         "env",
         "pkg",
         true,
      );

      // Same resource, same field, whichever way it was asked for. A reload
      // is exactly when a caller starts polling `status`, because a reload
      // invalidates the index — so this was the one response that omitted it.
      expect(withoutReload.embeddingIndex).toEqual(status);
      expect(withReload.embeddingIndex).toEqual(status);

      // And the reload really did reload: `getPackage(name, true)` is the
      // in-place recompile, so this is not the plain path in disguise.
      expect(reloaded.getPackage.calledWith("pkg", true)).toBe(true);
   });

   it("omits the field entirely when there is no index to describe", async () => {
      // `undefined` is how a server with no embedding provider answers. The
      // key must be ABSENT rather than null: the schema documents absence as
      // "no provider", so an explicit null would report a different fact.
      const { controller } = controllerWithIndex(undefined);
      const pkg = await controller.getPackage("env", "pkg", true);
      expect("embeddingIndex" in pkg).toBe(false);
   });
});

describe("PackageController.updatePackage reinstall decision", () => {
   afterEach(() => {
      sinon.restore();
   });

   const servedPackage = {
      getPackageMetadata: () => ({
         name: "pkg",
         location: "gs://bucket/pkg___1.0.0.zip",
      }),
   };

   function controllerWith(environment: object) {
      const getEnvironment = sinon
         .stub()
         .resolves({ getVersionService: () => null, ...environment });
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      });
      return {
         controller: new PackageController(
            environmentStore,
            testRetrieval(environmentStore),
         ),
         addPackageToDatabase,
      };
   }

   it("a PATCH whose location matches the installed one updates metadata without reinstalling", async () => {
      // The orchestrator's post-build rebind carries the package's own location
      // alongside the new manifestLocation. Nothing about the content changes
      // under one URI, so this must not re-download and recompile the package.
      const installPackage = sinon.stub().resolves(servedPackage);
      const updatePackage = sinon.stub().resolves({ name: "pkg" });
      const { controller, addPackageToDatabase } = controllerWith({
         peekPackage: () => servedPackage,
         awaitPackageLoads: async () => {},
         installPackage,
         updatePackage,
      });

      await controller.updatePackage("env", "pkg", {
         name: "pkg",
         location: "gs://bucket/pkg___1.0.0.zip",
         manifestLocation: "gs://bucket/pkg___1.0.0.manifest.json",
      });

      expect(installPackage.called).toBe(false);
      expect(updatePackage.calledOnce).toBe(true);
      expect(addPackageToDatabase.calledOnce).toBe(true);
   });

   it("a PATCH with a different location reinstalls, applying the body inside the install", async () => {
      const installPackage = sinon.stub().resolves(servedPackage);
      const updatePackage = sinon.stub().resolves({ name: "pkg" });
      const { controller } = controllerWith({
         peekPackage: () => servedPackage,
         awaitPackageLoads: async () => {},
         installPackage,
         updatePackage,
      });
      const body = {
         name: "pkg",
         location: "gs://bucket/pkg___1.0.1.zip",
      };

      await controller.updatePackage("env", "pkg", body);

      expect(installPackage.calledOnce).toBe(true);
      // The metadata rides with the install so both land under one lock hold;
      // a separate update call is exactly the second lock acquisition that a
      // queued delete could run between.
      expect(installPackage.firstCall.args[3]).toEqual({
         update: body,
         admit: expect.any(Function),
      });
      expect(updatePackage.called).toBe(false);
   });

   it("a PATCH matching the location of an install in flight waits for it and is then a metadata update", async () => {
      // Nothing is resident while the first install downloads. The decision is
      // made once the install has landed, against the copy it installed, so
      // the PATCH is a metadata update and not a second install of the same
      // location beside the first.
      const installPackage = sinon.stub().resolves(servedPackage);
      const updatePackage = sinon.stub().resolves({ name: "pkg" });
      let resident: typeof servedPackage | undefined;
      const { controller } = controllerWith({
         peekPackage: () => resident,
         awaitPackageLoads: async () => {
            resident = servedPackage; // the install landed while we waited
         },
         installPackage,
         updatePackage,
      });

      await controller.updatePackage("env", "pkg", {
         name: "pkg",
         location: "gs://bucket/pkg___1.0.0.zip",
         manifestLocation: "gs://bucket/pkg___1.0.0.manifest.json",
      });

      expect(installPackage.called).toBe(false);
      expect(updatePackage.calledOnce).toBe(true);
   });

   /** A package resident with `metadata`, as peekPackage returns it. */
   const residentWith = (metadata: Record<string, unknown>) => ({
      getPackageMetadata: () => ({
         name: "pkg",
         location: "gs://bucket/pkg___1.0.0.zip",
         ...metadata,
      }),
   });

   type ApiPackage = components["schemas"]["Package"];
   type Admit = (
      body: ApiPackage,
      target: ApiPackage,
      previous: ApiPackage | undefined,
   ) => ApiPackage;
   type Resident = { getPackageMetadata: () => Record<string, unknown> };

   /**
    * Send `body` to a package resident as `resident`. The environment's
    * installPackage runs the install's validator, then its `admit`, against a
    * fetched tree whose metadata is `fetched` and the resident it replaces,
    * as the real install does inside its swap; its updatePackage runs `admit`
    * against the resident, as the real one does under the lock. `applied()`
    * is the body that was admitted.
    */
   async function patch(
      body: Record<string, unknown>,
      resident: Resident | null = servedPackage,
      fetched: Record<string, unknown> = { name: "pkg" },
   ) {
      let admitted: ApiPackage | undefined;
      const installPackage = sinon
         .stub()
         .callsFake(
            async (
               _name: string,
               _download: unknown,
               validate: (pkg: object) => string | undefined,
               options: { update: ApiPackage; admit: Admit },
            ) => {
               validate({
                  getPackageMetadata: () => fetched,
                  formatInvalidExplores: () => "",
                  formatInvalidPersistencePolicy: () => "",
                  formatUnparseableAnnotations: () => "",
                  formatInvalidIncrementalPolicy: () => "",
                  formatInvalidPreaggregatePolicy: () => "",
                  formatPersistenceCollisionRejections: () => "",
               });
               admitted = options.admit(
                  options.update,
                  fetched as ApiPackage,
                  resident?.getPackageMetadata() as ApiPackage | undefined,
               );
               return servedPackage;
            },
         );
      const updatePackage = sinon
         .stub()
         .callsFake(
            async (
               _name: string,
               update: ApiPackage,
               options: { admit: Admit },
            ) => {
               admitted = options.admit(
                  update,
                  resident?.getPackageMetadata() as ApiPackage,
                  undefined,
               );
               return { name: "pkg" };
            },
         );
      const { controller, addPackageToDatabase } = controllerWith({
         peekPackage: () => resident ?? undefined,
         awaitPackageLoads: async () => {},
         installPackage,
         updatePackage,
      });
      const result = controller.updatePackage("env", "pkg", body as ApiPackage);
      return {
         result,
         installPackage,
         updatePackage,
         addPackageToDatabase,
         applied: () => admitted,
      };
   }

   async function expectRefused(
      body: Record<string, unknown>,
      resident?: Resident | null,
      fetched?: Record<string, unknown>,
   ) {
      const { result, addPackageToDatabase, applied } = await patch(
         body,
         resident,
         fetched,
      );
      await expect(result).rejects.toBeInstanceOf(InstalledFromLocationError);
      await expect(result).rejects.toThrow(/install.* from .*reload/);
      expect(applied()).toBeUndefined();
      expect(addPackageToDatabase.called).toBe(false);
   }

   it("refuses a PATCH that changes what a reload of a location-installed package would discard (409)", async () => {
      // A reload fetches the package again from its location and replaces its
      // files, keeping only the location and the manifest binding, so a change
      // to anything else would silently disappear on the next reload.
      for (const body of [
         {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.0.zip",
            description: "renamed",
         },
         { name: "pkg", description: "renamed" },
         { name: "pkg", queryableSources: "all" },
         { name: "pkg", scope: "version" },
         { name: "pkg", queryMetadata: { team: "x" } },
         { name: "pkg", materialization: { schedule: "0 * * * *" } },
      ]) {
         const { result, installPackage } = await patch(body);
         await expect(result).rejects.toBeInstanceOf(
            InstalledFromLocationError,
         );
         expect(installPackage.called).toBe(false);
         await expectRefused(body);
      }
   });

   it("answers the refusal with 409 and reason PACKAGE_INSTALLED_FROM_LOCATION", async () => {
      const { result } = await patch({ name: "pkg", description: "renamed" });
      const error = await result.catch((e: Error) => e);
      expect(internalErrorToHttpError(error as Error, { log: false })).toEqual({
         status: 409,
         json: {
            code: 409,
            message: expect.stringContaining("was installed from"),
            reason: "PACKAGE_INSTALLED_FROM_LOCATION",
         },
      });
   });

   it("refuses a PATCH that clears a field a reload would restore", async () => {
      // The PATCH applies an empty description, an empty listing and an empty
      // queryMetadata bag, so each is a change a reload would undo.
      const resident = residentWith({
         description: "Sales",
         explores: ["models/a.malloy"],
         queryMetadata: { team: "x" },
         materialization: { queryMetadata: { team: "x" } },
      });
      await expectRefused({ name: "pkg", description: "" }, resident);
      await expectRefused({ name: "pkg", explores: [] }, resident);
      await expectRefused({ name: "pkg", queryMetadata: {} }, resident);
   });

   it("accepts a PATCH whose empty or null fields the package already reads as", async () => {
      // Null is omitted, and an empty value on a field that is already empty
      // changes nothing: a generated client sends both for fields it was never
      // given.
      const { result, updatePackage } = await patch({
         name: "pkg",
         description: "",
         explores: [],
         queryableSources: null,
         scope: null,
         queryMetadata: null,
         materialization: null,
      });
      await result;
      expect(updatePackage.calledOnce).toBe(true);

      const set = residentWith({
         description: "Sales",
         explores: ["models/a.malloy"],
         scope: "version",
      });
      const nulls = await patch(
         {
            name: "pkg",
            description: null,
            explores: null,
            scope: null,
            materialization: null,
         },
         set,
      );
      await nulls.result;
      expect(nulls.updatePackage.calledOnce).toBe(true);
   });

   it("compares explores as the PATCH applies them, normalized", async () => {
      const resident = residentWith({ explores: ["models/a.malloy"] });
      const { result, updatePackage } = await patch(
         { name: "pkg", explores: ["./models/a.malloy"] },
         resident,
      );
      await result;
      expect(updatePackage.calledOnce).toBe(true);
      await expectRefused(
         { name: "pkg", explores: ["./models/b.malloy"] },
         resident,
      );
   });

   it("accepts a PATCH of a location-installed package that only echoes it back", async () => {
      const { result, updatePackage } = await patch({
         name: "pkg",
         location: "gs://bucket/pkg___1.0.0.zip",
         explores: [],
         scope: "package",
         queryableSources: "declared",
      });
      await result;
      expect(updatePackage.calledOnce).toBe(true);

      const full = {
         description: "Sales",
         explores: ["models/a.malloy"],
         queryableSources: "all",
         scope: "version",
         queryMetadata: { team: "x" },
         materialization: {
            schedule: "0 * * * *",
            queryMetadata: { team: "x" },
         },
      };
      const echoed = await patch({ name: "pkg", ...full }, residentWith(full));
      await echoed.result;
      expect(echoed.updatePackage.calledOnce).toBe(true);
   });

   it("refuses a PATCH that reinstalls from a new location with a setting the fetched tree does not declare", async () => {
      // The settings land in the new tree's publisher.json, and its next reload
      // fetches that tree again: judged against it, not the old one.
      await expectRefused({
         name: "pkg",
         location: "gs://bucket/pkg___1.0.1.zip",
         description: "next",
      });
      // A value that is neither the replaced package's nor the new tree's.
      await expectRefused(
         {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.1.zip",
            description: "Sales, renamed",
         },
         residentWith({ description: "Sales" }),
         { name: "pkg", description: "Sales 1.0.1" },
      );
   });

   it("accepts a whole package sent back with only its location changed, and serves the new tree's settings", async () => {
      // A client GETs the package, changes `location`, and PATCHes the whole
      // object back: every other setting echoes the package it replaces. Those
      // echoes are dropped, so the new source's own publisher.json stands and
      // nothing it says is lost on the next reload.
      const old = {
         description: "Sales",
         explores: ["models/a.malloy"],
         queryableSources: "all",
         scope: "version",
         queryMetadata: { team: "x" },
         materialization: {
            schedule: "0 * * * *",
            queryMetadata: { team: "x" },
         },
      };
      const fetched = {
         name: "pkg",
         description: "Sales 1.0.1",
         explores: ["models/b.malloy"],
         materialization: { schedule: "0 0 * * *" },
      };
      const { result, installPackage, applied } = await patch(
         {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.1.zip",
            manifestLocation: "gs://bucket/pkg___1.0.1.manifest.json",
            ...old,
         },
         residentWith(old),
         fetched,
      );
      await result;
      expect(installPackage.calledOnce).toBe(true);
      // Only what is not a setting a reload would lose is left to apply.
      expect(applied()).toEqual({
         name: "pkg",
         location: "gs://bucket/pkg___1.0.1.zip",
         manifestLocation: "gs://bucket/pkg___1.0.1.manifest.json",
      });
   });

   it("on a reinstall, keeps a setting equal to the new tree's and refuses one equal to neither", async () => {
      const resident = residentWith({ description: "Sales", scope: "package" });
      const fetched = { name: "pkg", description: "Sales 1.0.1" };
      const matches = await patch(
         {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.1.zip",
            description: "Sales 1.0.1",
            scope: "package",
         },
         resident,
         fetched,
      );
      await matches.result;
      expect(matches.installPackage.calledOnce).toBe(true);

      // An echo of the old package alongside a genuinely new value: the echo
      // is no reason to refuse, the new value is.
      const { result } = await patch(
         {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.1.zip",
            description: "Sales",
            scope: "version",
         },
         resident,
         fetched,
      );
      const error = await result.catch((e: Error) => e);
      expect(error).toBeInstanceOf(InstalledFromLocationError);
      expect((error as Error).message).toContain("change to scope.");
      expect((error as Error).message).not.toContain("description");
   });

   it("judges an echo against the replaced package only when it came from a location", async () => {
      // A package with no location owns its settings; a first install from a
      // location that echoed them would otherwise drop them silently.
      const local = {
         getPackageMetadata: () => ({ name: "pkg", description: "Mine" }),
      };
      await expectRefused(
         {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.0.zip",
            description: "Mine",
         },
         local,
         { name: "pkg", description: "Theirs" },
      );
   });

   it("refuses a PATCH that first installs a package from a location with settings", async () => {
      // No location before, one after: the next reload fetches from it.
      const local = { getPackageMetadata: () => ({ name: "pkg" }) };
      await expectRefused(
         {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.0.zip",
            explores: ["models/a.malloy"],
         },
         local,
      );
      await expectRefused(
         {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.0.zip",
            description: "first",
         },
         null,
      );
   });

   it("accepts a reinstall that carries only its location, or settings the fetched tree declares", async () => {
      const bare = await patch({
         name: "pkg",
         location: "gs://bucket/pkg___1.0.1.zip",
         manifestLocation: "gs://bucket/pkg___1.0.1.manifest.json",
         description: "",
         explores: [],
         scope: "package",
         versionId: "1.0.1",
      });
      await bare.result;
      expect(bare.installPackage.calledOnce).toBe(true);

      const declared = await patch(
         {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.1.zip",
            description: "Sales 1.0.1",
            explores: ["./models/a.malloy"],
         },
         servedPackage,
         {
            name: "pkg",
            description: "Sales 1.0.1",
            explores: ["models/a.malloy"],
         },
      );
      await declared.result;
      expect(declared.installPackage.calledOnce).toBe(true);
   });

   it("still applies metadata to a package that was not installed from a location", async () => {
      const local = { getPackageMetadata: () => ({ name: "pkg" }) };
      const updatePackage = sinon.stub().resolves({ name: "pkg" });
      const { controller } = controllerWith({
         peekPackage: () => local,
         awaitPackageLoads: async () => {},
         installPackage: sinon.stub(),
         updatePackage,
      });
      await controller.updatePackage("env", "pkg", {
         name: "pkg",
         description: "renamed",
      });
      expect(updatePackage.calledOnce).toBe(true);
   });

   it("a PATCH whose location is null is a metadata update", async () => {
      // A client that serializes unset fields as null names nothing to fetch.
      // Read as a location it would reach the downloader, which has no path to
      // make of null, and the request would fail 500.
      const installPackage = sinon.stub().resolves(servedPackage);
      const updatePackage = sinon.stub().resolves({ name: "pkg" });
      const { controller } = controllerWith({
         peekPackage: () => servedPackage,
         awaitPackageLoads: async () => {},
         installPackage,
         updatePackage,
      });

      await controller.updatePackage("env", "pkg", {
         name: "pkg",
         location: null as unknown as string,
         manifestLocation: "gs://bucket/pkg___1.0.0.manifest.json",
      });

      expect(installPackage.called).toBe(false);
      expect(updatePackage.calledOnce).toBe(true);
   });

   it("a PATCH naming the location of a reinstall in flight waits, and reinstalls only if that install failed", async () => {
      // While a reinstall from a new location runs, the resident copy still
      // names the old one. Decided then, the PATCH would start a second
      // install of the location already in flight. It waits instead, and is
      // decided against what the reinstall leaves resident: the new copy, so a
      // metadata update; or, after a rollback, the old copy, so a reinstall.
      const upgraded = {
         getPackageMetadata: () => ({
            name: "pkg",
            location: "gs://bucket/pkg___1.0.1.zip",
         }),
      };
      const body = { name: "pkg", location: "gs://bucket/pkg___1.0.1.zip" };

      let resident: typeof servedPackage = servedPackage; // …1.0.0.zip
      const succeeded = {
         installPackage: sinon.stub().resolves(upgraded),
         updatePackage: sinon.stub().resolves({ name: "pkg" }),
      };
      await controllerWith({
         peekPackage: () => resident,
         awaitPackageLoads: async () => {
            resident = upgraded;
         },
         ...succeeded,
      }).controller.updatePackage("env", "pkg", body);
      expect(succeeded.installPackage.called).toBe(false);
      expect(succeeded.updatePackage.calledOnce).toBe(true);

      const rolledBack = {
         installPackage: sinon.stub().resolves(upgraded),
         updatePackage: sinon.stub().resolves({ name: "pkg" }),
      };
      await controllerWith({
         peekPackage: () => servedPackage, // the rollback restored …1.0.0.zip
         awaitPackageLoads: async () => {},
         ...rolledBack,
      }).controller.updatePackage("env", "pkg", body);
      expect(rolledBack.installPackage.calledOnce).toBe(true);
      expect(rolledBack.updatePackage.called).toBe(false);
   });

   it("a PATCH that waited out a failed first install installs from its location", async () => {
      // Nothing resident once the wait ends means the install it waited for
      // failed; a PATCH that names a location then installs from it, as it
      // did before, rather than answering 404 for a package the caller is
      // trying to place here.
      const installPackage = sinon.stub().resolves(servedPackage);
      const updatePackage = sinon.stub().resolves({ name: "pkg" });
      const { controller } = controllerWith({
         peekPackage: () => undefined,
         awaitPackageLoads: async () => {},
         installPackage,
         updatePackage,
      });

      await controller.updatePackage("env", "pkg", {
         name: "pkg",
         location: "gs://bucket/pkg___1.0.0.zip",
      });

      expect(installPackage.calledOnce).toBe(true);
      expect(updatePackage.called).toBe(false);
   });

   it("a PATCH on a package not loaded here installs it", async () => {
      const installPackage = sinon.stub().resolves(servedPackage);
      const updatePackage = sinon.stub().resolves({ name: "pkg" });
      const { controller } = controllerWith({
         peekPackage: () => undefined,
         awaitPackageLoads: async () => {},
         installPackage,
         updatePackage,
      });

      await controller.updatePackage("env", "pkg", {
         name: "pkg",
         location: "gs://bucket/pkg___1.0.0.zip",
      });

      expect(installPackage.calledOnce).toBe(true);
      expect(updatePackage.called).toBe(false);
   });
});

describe("PackageController.reloadPackage", () => {
   afterEach(() => {
      sinon.restore();
   });

   it("a reload from the install location re-records that location on the reinstalled copy", async () => {
      // The re-fetched tree's publisher.json carries no `location`, so a
      // reinstall that does not write it back leaves the next same-location
      // PATCH reading as a change, and reinstalling again.
      const location = "gs://bucket/pkg___1.0.0.zip";
      const manifestLocation = "gs://bucket/pkg___1.0.0.manifest.json";
      // What callers set on the served copy since it was installed; the
      // re-fetched tree's publisher.json has none of it.
      const cached = {
         getPackageMetadata: () => ({
            name: "pkg",
            location,
            description: "set by a PATCH",
            manifestLocation,
            scope: "version" as const,
         }),
      };
      const reinstalled = {
         getPackageMetadata: () => ({ name: "pkg", location }),
         getOperationalFields: () => ({}),
      };
      const getPackage = sinon.stub().resolves(cached);
      const installPackage = sinon.stub().resolves(reinstalled);
      const environmentStore = fakeOf<PackageAdmin>({
         getEnvironment: sinon.stub().resolves({
            getVersionService: () => null,
            getPackage,
            installPackage,
         }),
      });
      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );
      sinon
         .stub(
            controller as unknown as { downloadInto: () => Promise<void> },
            "downloadInto",
         )
         .resolves();

      const result = await controller.reloadPackage("env", "pkg");

      expect(result.mode).toBe("reinstalled");
      expect(installPackage.calledOnce).toBe(true);
      // The location is re-recorded and the manifest binding re-applied
      // inside the install, or the package would serve live until the next
      // drift check rebinds it. Nothing else the served copy carried is: the
      // description, surface and policy are the re-fetched tree's to declare,
      // and re-applying them would run publish-time checks after the swap.
      expect(installPackage.firstCall.args[3]).toEqual({
         update: { location, manifestLocation },
      });
   });
});

describe("PackageController.addPackage manifestLocation", () => {
   afterEach(() => {
      sinon.restore();
   });

   const installedPackage = {
      formatInvalidExplores: () => "",
      formatInvalidPersistencePolicy: () => "",
      formatUnparseableAnnotations: () => "",
      formatInvalidIncrementalPolicy: () => "",
      formatInvalidPreaggregatePolicy: () => "",
      formatPersistenceCollisionRejections: () => "",
   };

   function addPackageController(environment: object) {
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment: sinon
            .stub()
            .resolves({ getVersionService: () => null, ...environment }),
         addPackageToDatabase: sinon.stub().resolves(undefined),
      });
      return new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );
   }

   it("hands the publish the body's location and manifestLocation", async () => {
      // The downloaded tree's publisher.json does not carry the manifest the
      // orchestrator computed, so the body is the only place it arrives; the
      // version is recorded bound to it. A null serves live.
      versioningOn();
      const versions = versionsPublishing(installedPackage);
      const controller = addPackageController({
         getVersionService: () => versions,
      });

      await controller.addPackage("env", {
         name: "pkg",
         location: "gs://bucket/pkg___1.0.0.zip",
         manifestLocation: "gs://bucket/pkg___1.0.0.manifest.json",
         description: "Sales",
      });
      expect(versions.publish.firstCall.args[0]).toBe("pkg");
      expect(versions.publish.firstCall.args[2]).toMatchObject({
         sourceLocation: "gs://bucket/pkg___1.0.0.zip",
         manifestLocation: "gs://bucket/pkg___1.0.0.manifest.json",
         description: "Sales",
         promotion: "on-publish",
      });

      await controller.addPackage("env", {
         name: "pkg",
         location: "gs://bucket/pkg___1.0.0.zip",
         manifestLocation: null,
      });
      expect(versions.publish.secondCall.args[2].manifestLocation).toBeNull();
   });

   it("a null manifestLocation is not forwarded into the install", async () => {
      // A fresh install serves live already. Forwarded, a null would take the
      // revert-to-live branch and recompile every model a second time inside
      // the install's lock hold, and write `manifestLocation: null` to disk.
      const installPackage = sinon.stub().resolves(installedPackage);
      const controller = addPackageController({ installPackage });

      await controller.addPackage("env", {
         name: "pkg",
         location: "gs://bucket/pkg___1.0.0.zip",
         manifestLocation: null,
      });

      expect(installPackage.firstCall.args[3]).toEqual({
         update: { location: "gs://bucket/pkg___1.0.0.zip" },
      });
   });

   it("a publish with a location binds the body's manifestLocation as part of the install", async () => {
      // The downloaded tree's publisher.json does not carry the manifest the
      // orchestrator computed, so the body is the only place it arrives. Left
      // unapplied, the package came up unbound and was fully reloaded by the
      // next drift check.
      const installPackage = sinon.stub().resolves({
         formatInvalidExplores: () => "",
         formatInvalidPersistencePolicy: () => "",
         formatUnparseableAnnotations: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      });
      const environment = { getVersionService: () => null, installPackage };
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment: sinon.stub().resolves(environment),
         addPackageToDatabase: sinon.stub().resolves(undefined),
      });
      const controller = new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );

      await controller.addPackage("env", {
         name: "pkg",
         location: "gs://bucket/pkg___1.0.0.zip",
         manifestLocation: "gs://bucket/pkg___1.0.0.manifest.json",
      });

      expect(installPackage.calledOnce).toBe(true);
      expect(installPackage.firstCall.args[3]).toEqual({
         update: {
            location: "gs://bucket/pkg___1.0.0.zip",
            manifestLocation: "gs://bucket/pkg___1.0.0.manifest.json",
         },
      });
   });

   it("an admission refusal is answered, not recorded as a load failure", async () => {
      // The refusal is this request's answer; the caller places the package
      // elsewhere. Recorded, it would sit in /status's load errors until this
      // server next loaded that package, which it may never do. A failure of
      // the server's own (any other 5xx) is still recorded.
      const recordPackageAddFailure = sinon.stub();
      const refused = sinon
         .stub()
         .rejects(new PackageAdmissionRefusedError("under memory pressure"));
      const refusing = addPackageController({
         installPackage: refused,
         recordPackageAddFailure,
      });
      await expect(
         refusing.addPackage("env", {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.0.zip",
         }),
      ).rejects.toBeInstanceOf(ServiceUnavailableError);
      expect(recordPackageAddFailure.called).toBe(false);

      // Any other 5xx is the server's own problem and is recorded, including
      // a worker-pool failure that is also a ServiceUnavailableError: its
      // message carries the cause (an errno) an operator has to fix.
      const failing = addPackageController({
         installPackage: sinon.stub().rejects(new Error("bucket unreachable")),
         recordPackageAddFailure,
      });
      await expect(
         failing.addPackage("env", {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.0.zip",
         }),
      ).rejects.toThrow("bucket unreachable");
      expect(recordPackageAddFailure.calledOnce).toBe(true);

      const workerPool = addPackageController({
         installPackage: sinon
            .stub()
            .rejects(
               new ServiceUnavailableError(
                  "Package-load worker pool unavailable: EACCES: permission denied",
               ),
            ),
         recordPackageAddFailure,
      });
      await expect(
         workerPool.addPackage("env", {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.0.zip",
         }),
      ).rejects.toBeInstanceOf(ServiceUnavailableError);
      expect(recordPackageAddFailure.calledTwice).toBe(true);
   });

   it("a versioned publish: an admission refusal is answered, not recorded as a load failure", async () => {
      // The refusal is this request's answer; the caller places the package
      // elsewhere. Recorded, it would sit in /status's load errors until this
      // server next loaded that package, which it may never do. A failure of
      // the server's own (any other 5xx) is still recorded.
      versioningOn();
      const recordPackageAddFailure = sinon.stub();
      const refusing = addPackageController({
         getVersionService: () =>
            versionsPublishing(
               installedPackage,
               new PackageAdmissionRefusedError("under memory pressure"),
            ),
         recordPackageAddFailure,
      });
      await expect(
         refusing.addPackage("env", {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.0.zip",
         }),
      ).rejects.toBeInstanceOf(ServiceUnavailableError);
      expect(recordPackageAddFailure.called).toBe(false);

      // Any other 5xx is the server's own problem and is recorded, including
      // a worker-pool failure that is also a ServiceUnavailableError: its
      // message carries the cause (an errno) an operator has to fix.
      const failing = addPackageController({
         getVersionService: () =>
            versionsPublishing(
               installedPackage,
               new Error("bucket unreachable"),
            ),
         recordPackageAddFailure,
      });
      await expect(
         failing.addPackage("env", {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.0.zip",
         }),
      ).rejects.toThrow("bucket unreachable");
      expect(recordPackageAddFailure.calledOnce).toBe(true);

      const workerPool = addPackageController({
         getVersionService: () =>
            versionsPublishing(
               installedPackage,
               new ServiceUnavailableError(
                  "Package-load worker pool unavailable: EACCES: permission denied",
               ),
            ),
         recordPackageAddFailure,
      });
      await expect(
         workerPool.addPackage("env", {
            name: "pkg",
            location: "gs://bucket/pkg___1.0.0.zip",
         }),
      ).rejects.toBeInstanceOf(ServiceUnavailableError);
      expect(recordPackageAddFailure.calledTwice).toBe(true);
   });
});

describe("PackageController.updatePackage on a versioned package", () => {
   afterEach(() => {
      sinon.restore();
   });

   function versionedPatchController() {
      const loaded = { name: "pkg" };
      const versions = {
         isVersioned: async () => true,
         latestOf: async () => "2.0.0",
         getVersion: async () => ({
            sourceLocation: "gs://bucket/pkg-2.0.0.zip",
         }),
         setManifest: sinon.stub().resolves(loaded),
         setPackageDescription: sinon.stub().resolves(undefined),
      };
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment: sinon.stub().resolves({
            getVersionService: () => versions,
            getPackage: sinon.stub().resolves(loaded),
            describePackage: sinon
               .stub()
               .resolves({ name: "pkg", versionId: "2.0.0" }),
         }),
      });
      return {
         controller: new PackageController(
            environmentStore,
            testRetrieval(environmentStore),
         ),
         versions,
      };
   }

   it("rebinds latest to a manifest URI, and leaves the binding alone on a null or empty one", async () => {
      // A generated client serializes a field it was never given as null, so
      // a null is unset here, as it is on a publish. Read as a clear, it
      // would turn latest back to serving live with a 200. Clearing is
      // PUT .../versions/{version}/manifest's.
      const { controller, versions } = versionedPatchController();
      for (const manifestLocation of [null, ""]) {
         const patched = await controller.updatePackage("env", "pkg", {
            name: null as unknown as string,
            location: null as unknown as string,
            manifestLocation,
            description: null as unknown as string,
         });
         expect(patched).toMatchObject({ versionId: "2.0.0" });
      }
      expect(versions.setManifest.called).toBe(false);

      await controller.updatePackage("env", "pkg", {
         manifestLocation: "gs://bucket/pkg-2.0.0.manifest.json",
      });
      expect(versions.setManifest.firstCall.args).toEqual([
         "pkg",
         "2.0.0",
         "gs://bucket/pkg-2.0.0.manifest.json",
      ]);
   });
});

describe("PackageController.addPackage with package versioning off or on", () => {
   const installedPackage = {
      formatInvalidExplores: () => "",
      formatInvalidPersistencePolicy: () => "",
      formatUnparseableAnnotations: () => "",
      formatInvalidIncrementalPolicy: () => "",
      formatInvalidPreaggregatePolicy: () => "",
      formatPersistenceCollisionRejections: () => "",
   };

   afterEach(() => {
      sinon.restore();
   });

   function controllerWith(versions: object, installPackage: sinon.SinonStub) {
      const environmentStore = fakeOf<PackageAdmin>({
         publisherConfigIsFrozen: false,
         getEnvironment: sinon.stub().resolves({
            getVersionService: () => versions,
            installPackage,
         }),
         addPackageToDatabase: sinon.stub().resolves(undefined),
      });
      return new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );
   }

   // logger.warn may already be a spy another spec left in place (health.spec
   // never restores its own), and spying on it again returns that spy with the
   // calls it holds; clear them so a test counts only its own.
   const spyOnWarn = () => {
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      warn.mockClear();
      return warn;
   };

   const offWarnings = (warn: { mock: { calls: unknown } }) =>
      (warn.mock.calls as unknown as unknown[][]).filter(([message]) =>
         String(message).includes("Package versioning is off"),
      );

   it("off: installs in place as before, whatever the tree declares, and warns", async () => {
      // What every publish from a location did before versions, with the
      // install's own arguments. The version service is never asked: off, a
      // publisher.json's version is not read.
      versioning("off");
      const versions = versionsPublishing(installedPackage);
      const installPackage = sinon.stub().resolves(installedPackage);
      const warn = spyOnWarn();
      try {
         await controllerWith(versions, installPackage).addPackage("env", {
            name: "pkg",
            location: "gs://bucket/pkg.zip",
            manifestLocation: null,
         });

         expect(installPackage.calledOnce).toBe(true);
         expect(installPackage.firstCall.args[0]).toBe("pkg");
         expect(installPackage.firstCall.args[3]).toEqual({
            update: { location: "gs://bucket/pkg.zip" },
         });
         expect(versions.publish.called).toBe(false);
         const warned = offWarnings(warn);
         expect(warned).toHaveLength(1);
         expect(String(warned[0][0])).toContain("package pkg");
         expect(String(warned[0][0])).toContain(
            "PUBLISHER_PACKAGE_VERSIONING=on",
         );
         expect(warned[0][1]).toEqual({ packageName: "pkg" });
      } finally {
         warn.mockRestore();
      }
   });

   it("off: warns once per package, not on every republish", async () => {
      versioning("off");
      const controller = controllerWith(
         versionsPublishing(installedPackage),
         sinon.stub().resolves(installedPackage),
      );
      const warn = spyOnWarn();
      try {
         for (const name of ["pkg", "pkg", "other", "pkg"]) {
            await controller.addPackage("env", {
               name,
               location: `gs://bucket/${name}.zip`,
            });
         }
         expect(
            offWarnings(warn).map(
               ([, meta]) => (meta as { packageName: string }).packageName,
            ),
         ).toEqual(["pkg", "other"]);
      } finally {
         warn.mockRestore();
      }
   });

   it("off: refuses to replace a package that has published versions, naming the flag", async () => {
      versioning("off");
      const versions = {
         ...versionsPublishing(installedPackage),
         isVersioned: async () => true,
      };
      const installPackage = sinon.stub().resolves(installedPackage);
      const refused = await controllerWith(versions, installPackage)
         .addPackage("env", { name: "pkg", location: "/srv/pkg" })
         .then(
            () => undefined,
            (err: unknown) => err,
         );
      expect(refused).toBeInstanceOf(PackageVersionError);
      expect((refused as PackageVersionError).reason).toBe(
         "PACKAGE_IS_VERSIONED",
      );
      expect((refused as Error).message).toContain(
         "PUBLISHER_PACKAGE_VERSIONING=on",
      );
      expect(installPackage.called).toBe(false);
      expect(versions.publish.called).toBe(false);
   });

   it("on: publishes the tree as a version, never installing in place or warning", async () => {
      versioningOn();
      const versions = versionsPublishing(installedPackage);
      const installPackage = sinon.stub().resolves(installedPackage);
      const warn = spyOnWarn();
      try {
         await controllerWith(versions, installPackage).addPackage("env", {
            name: "pkg",
            location: "gs://bucket/pkg.zip",
         });
         expect(versions.publish.calledOnce).toBe(true);
         expect(installPackage.called).toBe(false);
         expect(offWarnings(warn)).toHaveLength(0);
      } finally {
         warn.mockRestore();
      }
   });

   it("on: a tree that declares no version is refused with the version service's 400", async () => {
      versioningOn();
      const versions = versionsPublishing(
         installedPackage,
         new PackageVersionError("MANIFEST_VERSION_MISSING", "no version"),
      );
      const installPackage = sinon.stub().resolves(installedPackage);
      const refused = await controllerWith(versions, installPackage)
         .addPackage("env", { name: "pkg", location: "gs://bucket/pkg.zip" })
         .then(
            () => undefined,
            (err: unknown) => err,
         );
      expect((refused as PackageVersionError).reason).toBe(
         "MANIFEST_VERSION_MISSING",
      );
      expect(installPackage.called).toBe(false);
   });
});

/**
 * A version service whose publish fails with `failWith` (as a download, or a
 * tree with no version, does), or otherwise runs the publish checks it is
 * handed on `loaded`, as the real one does before it records anything, and
 * answers with it.
 */
function versionsPublishing(loaded: unknown, failWith?: Error) {
   return {
      isVersioned: async () => false,
      publish: sinon
         .stub()
         .callsFake(
            async (
               _packageName: string,
               _downloader: unknown,
               options: { validate?: (pkg: unknown) => string | undefined },
            ) => {
               if (failWith) throw failWith;
               const msg = options.validate?.(loaded);
               if (msg) throw new BadRequestError(msg);
               return { loaded, version: {}, created: true };
            },
         ),
   };
}

describe("PackageController.getPackageBuildPlan", () => {
   function controllerServing(pkg: {
      getBuildPlan: () => unknown;
      isBuildPlanUnavailable: () => boolean;
      whenBuildPlanCurrent?: () => Promise<void>;
   }): PackageController {
      const environmentStore = fakeOf<PackageAdmin>({
         getEnvironment: sinon.stub().resolves({
            getPackage: sinon.stub().resolves({
               whenBuildPlanCurrent: () => Promise.resolve(),
               ...pkg,
            }),
         }),
      });
      return new PackageController(
         environmentStore,
         testRetrieval(environmentStore),
      );
   }

   it("is the load's plan", async () => {
      // The very object the load derived, not a copy.
      const plan = { graphs: [], sources: {} };
      const controller = controllerServing({
         getBuildPlan: () => plan,
         isBuildPlanUnavailable: () => false,
      });
      expect(await controller.getPackageBuildPlan("env", "pkg")).toBe(plan);
   });

   it("waits for a re-derivation a connection edit started (ms2data/service#6145)", async () => {
      // A connection edit can move content addresses; the route must never
      // hand out the plan from before it.
      const stale = { graphs: [], sources: { s: { sourceEntityId: "old" } } };
      const fresh = { graphs: [], sources: { s: { sourceEntityId: "new" } } };
      let current: unknown = stale;
      let finish!: () => void;
      const refreshed = new Promise<void>((resolve) => {
         finish = () => {
            current = fresh;
            resolve();
         };
      });
      const controller = controllerServing({
         getBuildPlan: () => current,
         isBuildPlanUnavailable: () => false,
         whenBuildPlanCurrent: () => refreshed,
      });
      const read = controller.getPackageBuildPlan("env", "pkg");
      // Let the route reach the plan read before the refresh lands.
      await new Promise((resolve) => setTimeout(resolve, 10));
      finish();
      expect(await read).toBe(fresh as unknown as Awaited<typeof read>);
   });

   it("answers 503 with Retry-After when the re-derivation outlasts the wait (BuildPlanRefreshingError)", async () => {
      const controller = controllerServing({
         getBuildPlan: () => null,
         isBuildPlanUnavailable: () => false,
         whenBuildPlanCurrent: () =>
            Promise.reject(new BuildPlanRefreshingError("refreshing")),
      });
      const err = await controller
         .getPackageBuildPlan("env", "pkg")
         .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(BuildPlanRefreshingError);
      const answer = internalErrorToHttpError(err as Error, { log: false });
      expect(answer.status).toBe(503);
      expect(answer.headers).toEqual({ "Retry-After": "5" });
      expect(answer.json.reason).toBe("BUILD_PLAN_REFRESHING");
   });

   it("is an empty plan when the package declares no persist source", async () => {
      const controller = controllerServing({
         getBuildPlan: () => null,
         isBuildPlanUnavailable: () => false,
      });
      expect(await controller.getPackageBuildPlan("env", "pkg")).toEqual({
         graphs: [],
         sources: {},
      });
   });

   it("is a 409 BUILD_PLAN_UNAVAILABLE when the plan could not be derived at load", async () => {
      const controller = controllerServing({
         getBuildPlan: () => null,
         isBuildPlanUnavailable: () => true,
      });
      const error = await controller.getPackageBuildPlan("env", "pkg").then(
         () => undefined,
         (e: unknown) => e,
      );
      expect(error).toBeInstanceOf(BuildPlanUnavailableError);
      expect(internalErrorToHttpError(error as Error)).toMatchObject({
         status: 409,
         json: { reason: "BUILD_PLAN_UNAVAILABLE" },
      });
   });
});

describe("PackageController.dryRunPackage", () => {
   type DryRunOptions = {
      validate?: (pkg: unknown) => string | undefined;
      readVersion: boolean;
      describe: (pkg: unknown, versionId: string | undefined) => unknown;
   };

   /** A controller whose environment answers a dry run with `answer`. */
   function controllerWith(
      answer: (options: DryRunOptions) => Promise<unknown>,
      { frozen = false, versioned = false } = {},
   ) {
      const seen: DryRunOptions[] = [];
      const environment = {
         getVersionService: () => (versioned ? {} : null),
         dryRunInstall: (
            _name: string,
            _download: unknown,
            options: DryRunOptions,
         ) => {
            seen.push(options);
            return answer(options);
         },
      };
      const getEnvironment = sinon.stub().resolves(environment);
      const controller = new PackageController(
         fakeOf<PackageAdmin>({
            publisherConfigIsFrozen: frozen,
            getEnvironment,
         }),
         testRetrieval(
            fakeOf<PackageAdmin>({
               publisherConfigIsFrozen: frozen,
               getEnvironment,
            }),
         ),
      );
      return { controller, seen, getEnvironment };
   }

   const loadedPackage = {
      getPackageMetadata: () => ({ name: "pkg", warnings: [] }),
      previewServeVerdicts: async () => [],
      getBuildPlan: () => null,
   };
   const body = { name: "pkg", location: "gs://bucket/pkg.zip" };

   it("is refused on a frozen server, as a publish is, before anything is loaded", async () => {
      const { controller, getEnvironment } = controllerWith(async () => ({}), {
         frozen: true,
      });
      await expect(
         controller.dryRunPackage("env", body),
      ).rejects.toBeInstanceOf(FrozenConfigError);
      expect(getEnvironment.called).toBe(false);
   });

   it("answers the package as it would load, with the location it came from and no serving state", async () => {
      const { controller, seen } = controllerWith(async (options) =>
         options.describe(loadedPackage, undefined),
      );
      const answer = await controller.dryRunPackage("env", body);
      expect(answer).toEqual({
         name: "pkg",
         warnings: [],
         location: "gs://bucket/pkg.zip",
      });
      expect(seen[0].readVersion).toBe(false);
   });

   // ms2data/service#7090: what a publish would build and serve.
   it("adds the build plan and the previewed serve verdicts when the package materializes", async () => {
      const buildPlan = { graphs: [], sources: {} };
      const verdict = {
         sourceName: "orders",
         sourceID: "orders@file:///pkg/m.malloy",
         origin: "persist",
         tier: "colocated",
         verdict: "not_built",
         message: "`orders` will serve from its table once a build binds it.",
      };
      const { controller } = controllerWith(async (options) =>
         options.describe(
            {
               ...loadedPackage,
               previewServeVerdicts: async () => [verdict],
               getBuildPlan: () => buildPlan,
            },
            undefined,
         ),
      );
      const answer = await controller.dryRunPackage("env", body);
      expect(answer).toMatchObject({ buildPlan, serveVerdicts: [verdict] });
   });

   it("with versioning on, reads the tree's version and names it in the answer", async () => {
      versioningOn();
      const { controller, seen } = controllerWith(
         async (options) => options.describe(loadedPackage, "1.2.0"),
         { versioned: true },
      );
      const answer = await controller.dryRunPackage("env", body);
      expect(seen[0].readVersion).toBe(true);
      expect((answer as { versionId?: string }).versionId).toBe("1.2.0");
   });

   it("with versioning on, refuses the request fields a versioned publish refuses, before loading anything", async () => {
      versioningOn();
      const { controller, seen } = controllerWith(
         async (options) => options.describe(loadedPackage, "1.2.0"),
         { versioned: true },
      );
      await expect(
         controller.dryRunPackage("env", {
            ...body,
            manifestLocation: "relative/manifest.json",
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
      await expect(
         controller.dryRunPackage("env", {
            ...body,
            description: 7 as unknown as string,
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(seen).toHaveLength(0);
      // A manifest location it can read, and null (serve live), are accepted.
      await controller.dryRunPackage("env", {
         ...body,
         manifestLocation: "gs://bucket/manifest.json",
      });
      expect(seen).toHaveLength(1);
   });

   it("does not read a version when versioning is off", async () => {
      versioning("off");
      const { controller, seen } = controllerWith(
         async (options) => options.describe(loadedPackage, undefined),
         { versioned: true },
      );
      await controller.dryRunPackage("env", body);
      expect(seen[0].readVersion).toBe(false);
   });

   it("classifies a failure without logging it, since the route logs it once", async () => {
      const errorLog = spyOn(logger, "error");
      try {
         const { controller } = controllerWith(async () => {
            throw new Error("EACCES: /var/publisher");
         });
         await expect(controller.dryRunPackage("env", body)).rejects.toThrow(
            "EACCES",
         );
         expect(errorLog).not.toHaveBeenCalled();
      } finally {
         errorLog.mockRestore();
      }
   });

   it("passes a memory-backpressure refusal through as the publish would answer it", async () => {
      const { controller } = controllerWith(async () => {
         throw new PackageAdmissionRefusedError("memory pressure");
      });
      const error = await controller
         .dryRunPackage("env", body)
         .catch((e: unknown) => e);
      expect(error).toBeInstanceOf(PackageAdmissionRefusedError);
      expect(
         internalErrorToHttpError(error as Error, { log: false }).status,
      ).toBe(503);
   });
});
