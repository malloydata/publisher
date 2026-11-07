// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import sinon from "sinon";

import type { components } from "../api";
import {
   BadRequestError,
   FrozenConfigError,
   PackageAdmissionRefusedError,
   PackageVersionError,
   BuildPlanUnavailableError,
   internalErrorToHttpError,
   ServiceUnavailableError,
} from "../errors";
import { logger } from "../logger";
import { Environment } from "../service/environment";
import type { EnvironmentStore } from "../service/environment_store";
import { installSettings } from "../test_helpers/settings_env";
import { PackageController } from "./package.controller";

let restoreSettings: (() => void) | undefined;

// Every test runs with no settings set, never the developer's shell: a
// publish reads its promotion mode from them.
beforeEach(() => {
   restoreSettings = installSettings().restore;
});

afterEach(() => {
   restoreSettings?.();
   restoreSettings = undefined;
});

// DEPRECATED FEATURE COVERAGE — delete together with `explores` (see RELEASE_NOTES deprecations).
describe("deprecated: explores — PackageController.addPackage validation", () => {
   afterEach(() => {
      sinon.restore();
   });

   it("location: a published version runs the publish checks before it is recorded, not as a controller delete", async () => {
      // A publish from a location is a published version: its tree was just
      // downloaded, and the version service runs the checks before anything
      // is recorded, so a refusal leaves nothing to delete or unload.
      const invalidMsg =
         "Invalid explores entry 'missing.malloy' in publisher.json: file not found";
      const mockPackage = {
         formatInvalidExplores: () => invalidMsg,
         formatInvalidPersistencePolicy: () => "",
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
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);

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
   });

   it("location: publishes the version when explores are valid", async () => {
      const mockPackage = {
         formatInvalidExplores: () => "",
         formatInvalidPersistencePolicy: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const versions = versionsPublishing(mockPackage);
      const getEnvironment = sinon
         .stub()
         .resolves({ getVersionService: () => versions });
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);

      const published = await controller.addPackage("env", {
         name: "pkg",
         description: "test",
         location: "gs://bucket/pkg.zip",
         explores: ["index.malloy"],
      });

      expect(versions.publish.calledOnce).toBe(true);
      expect(published as unknown).toBe(mockPackage);
   });
});

describe("PackageController.addPackage persistence policy validation", () => {
   afterEach(() => {
      sinon.restore();
   });

   it("location path: the persistence-policy gate refuses the version before it is recorded", async () => {
      const cronMsg =
         'materialization.schedule (cron) in publisher.json requires "scope": ' +
         '"version".';
      const mockPackage = {
         formatInvalidExplores: () => "",
         formatInvalidPersistencePolicy: () => cronMsg,
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const environment = {
         getVersionService: () => versionsPublishing(mockPackage),
      };
      const getEnvironment = sinon.stub().resolves(environment);
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);

      await expect(
         controller.addPackage("env", {
            name: "pkg",
            description: "test",
            location: "gs://bucket/pkg.zip",
         }),
      ).rejects.toThrow(cronMsg);
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
         formatInvalidIncrementalPolicy: () => incrementalMsg,
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const versions = versionsPublishing(mockPackage);
      const getEnvironment = sinon.stub().resolves({
         getVersionService: () => versions,
      });
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);

      const error = await controller
         .addPackage("env", {
            name: "pkg",
            description: "test",
            location: "gs://bucket/pkg.zip",
         })
         .then(
            () => undefined,
            (err: unknown) => err as Error,
         );

      expect(error).toBeInstanceOf(BadRequestError);
      expect(error!.message).toBe(`${cronMsg}\n${incrementalMsg}`);
      expect(versions.publish.calledOnce).toBe(true);
   });

   it("publishes when the incremental declaration is the only thing declared and it is valid", async () => {
      const mockPackage = {
         formatInvalidExplores: () => "",
         formatInvalidPersistencePolicy: () => "",
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const versions = versionsPublishing(mockPackage);
      const getEnvironment = sinon
         .stub()
         .resolves({ getVersionService: () => versions });
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);
      const published = await controller.addPackage("env", {
         name: "pkg",
         description: "test",
         location: "gs://bucket/pkg.zip",
      });

      expect(versions.publish.calledOnce).toBe(true);
      expect(published as unknown).toBe(mockPackage);
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
      const environmentStore = {
         getEnvironment,
      } as unknown as EnvironmentStore;
      const controller = new PackageController(environmentStore);
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

describe("PackageController.reloadPackage", () => {
   afterEach(() => {
      sinon.restore();
   });

   it("a package that is not a published version recompiles in place, and is never re-fetched from where it came from", async () => {
      // A package served from its configuration may name the location it came
      // from. A reload recompiles the tree on disk; it never downloads again.
      const recompiled = {
         getPackageMetadata: () => ({
            name: "pkg",
            location: "gs://bucket/pkg.zip",
         }),
         getOperationalFields: () => ({ servedRevision: "rev-2" }),
      };
      const getPackage = sinon.stub().resolves(recompiled);
      const resolve = sinon.stub().resolves(null);
      const downloadPackageInto = sinon.stub().resolves();
      const controller = new PackageController({
         getEnvironment: sinon.stub().resolves({
            getVersionService: () => ({ resolve }),
            getPackage,
         }),
         downloadPackageInto,
      } as unknown as EnvironmentStore);

      const result = await controller.reloadPackage("env", "pkg");

      expect(result.mode).toBe("in-place");
      expect(result.operational).toEqual({ servedRevision: "rev-2" });
      expect(resolve.calledOnceWith("pkg", undefined)).toBe(true);
      expect(getPackage.calledOnceWith("pkg", true)).toBe(true);
      expect(downloadPackageInto.called).toBe(false);
   });

   it("a published version is answered unchanged, recompiling nothing", async () => {
      // A published version is immutable: the reload reads the version named
      // (latest when none is) and describes it as a GET would.
      const version = {
         getOperationalFields: () => ({ servedRevision: "rev-1" }),
      };
      const getPackage = sinon.stub().resolves(version);
      const describePackage = sinon
         .stub()
         .resolves({ name: "pkg", versionId: "1.0.0", latestVersion: "1.0.0" });
      const controller = new PackageController({
         getEnvironment: sinon.stub().resolves({
            getVersionService: () => ({
               resolve: async () => ({ versionId: "1.0.0" }),
            }),
            getPackage,
            describePackage,
         }),
      } as unknown as EnvironmentStore);

      const result = await controller.reloadPackage(
         "env",
         "pkg",
         "rest",
         "1.0.0",
      );

      expect(result).toEqual({
         metadata: { name: "pkg", versionId: "1.0.0", latestVersion: "1.0.0" },
         operational: { servedRevision: "rev-1" },
         mode: "unchanged",
      });
      expect(
         getPackage.calledOnceWith("pkg", false, { versionId: "1.0.0" }),
      ).toBe(true);
      expect(describePackage.calledOnceWith(version)).toBe(true);
   });
});

describe("PackageController.addPackage manifestLocation", () => {
   afterEach(() => {
      sinon.restore();
   });

   const installedPackage = {
      formatInvalidExplores: () => "",
      formatInvalidPersistencePolicy: () => "",
      formatInvalidIncrementalPolicy: () => "",
      formatInvalidPreaggregatePolicy: () => "",
      formatPersistenceCollisionRejections: () => "",
   };

   function addPackageController(environment: object) {
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment: sinon
            .stub()
            .resolves({ getVersionService: () => null, ...environment }),
      } as unknown as EnvironmentStore;
      return new PackageController(environmentStore);
   }

   it("hands the publish the body's location and manifestLocation", async () => {
      // The downloaded tree's publisher.json does not carry the manifest the
      // orchestrator computed, so the body is the only place it arrives; the
      // version is recorded bound to it. A null serves live.
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

   it("an admission refusal is answered, not recorded as a load failure", async () => {
      // The refusal is this request's answer; the caller places the package
      // elsewhere. Recorded, it would sit in /status's load errors until this
      // server next loaded that package, which it may never do. A failure of
      // the server's own (any other 5xx) is still recorded.
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

describe("PackageController.addPackage publishes a version", () => {
   const installedPackage = {
      formatInvalidExplores: () => "",
      formatInvalidPersistencePolicy: () => "",
      formatInvalidIncrementalPolicy: () => "",
      formatInvalidPreaggregatePolicy: () => "",
      formatPersistenceCollisionRejections: () => "",
   };

   afterEach(() => {
      sinon.restore();
   });

   function controllerWith(versions: object) {
      const getEnvironment = sinon.stub().resolves({
         getVersionService: () => versions,
         recordPackageAddFailure: sinon.stub(),
      });
      const downloadPackageInto = sinon.stub().resolves();
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
         downloadPackageInto,
      } as unknown as EnvironmentStore;
      return {
         controller: new PackageController(environmentStore),
         getEnvironment,
         downloadPackageInto,
      };
   }

   it("refuses a request without a location with 400, before anything is read or downloaded", async () => {
      // Every package is a published version, so there is no directory to
      // register in place: the tree to publish is the request's `location`.
      const versions = versionsPublishing(installedPackage);
      const { controller, getEnvironment, downloadPackageInto } =
         controllerWith(versions);
      const cases: [unknown, string][] = [
         [undefined, "A publish needs `location`"],
         [null, "A publish needs `location`"],
         ["", "A publish needs `location`"],
         [7, "`location` must be a string."],
      ];
      for (const [location, message] of cases) {
         const refused = await controller
            .addPackage("env", {
               name: "pkg",
               location: location as unknown as string,
            })
            .then(
               () => undefined,
               (err: unknown) => err,
            );
         expect(refused).toBeInstanceOf(BadRequestError);
         expect((refused as Error).message).toContain(message);
         expect(internalErrorToHttpError(refused as Error).status).toBe(400);
      }
      expect(getEnvironment.called).toBe(false);
      expect(versions.publish.called).toBe(false);
      expect(downloadPackageInto.called).toBe(false);
   });

   it("publishes the tree at the location as the version it declares, handing the version service a downloader for that location", async () => {
      const versions = versionsPublishing(installedPackage);
      const { controller, downloadPackageInto } = controllerWith(versions);

      const published = await controller.addPackage("env", {
         name: "pkg",
         location: "gs://bucket/pkg.zip",
      });

      expect(published as unknown).toBe(installedPackage);
      expect(versions.publish.calledOnce).toBe(true);
      expect(versions.publish.firstCall.args[0]).toBe("pkg");
      // The request carries no version: the version service reads it from the
      // tree the downloader writes.
      await versions.publish.firstCall.args[1]("/staging/pkg");
      expect(downloadPackageInto.firstCall.args).toEqual([
         "env",
         "pkg",
         "gs://bucket/pkg.zip",
         "/staging/pkg",
      ]);
   });

   it("a tree that declares no version is refused with the version service's 400", async () => {
      const versions = versionsPublishing(
         installedPackage,
         new PackageVersionError("MANIFEST_VERSION_MISSING", "no version"),
      );
      const refused = await controllerWith(versions)
         .controller.addPackage("env", {
            name: "pkg",
            location: "gs://bucket/pkg.zip",
         })
         .then(
            () => undefined,
            (err: unknown) => err,
         );
      expect((refused as PackageVersionError).reason).toBe(
         "MANIFEST_VERSION_MISSING",
      );
      expect(internalErrorToHttpError(refused as Error).status).toBe(400);
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
   }): PackageController {
      const environmentStore = {
         getEnvironment: sinon.stub().resolves({
            getPackage: sinon.stub().resolves(pkg),
         }),
      } as unknown as EnvironmentStore;
      return new PackageController(environmentStore);
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
      describe: (pkg: unknown, versionId: string) => unknown;
   };

   /** A controller whose environment answers a dry run with `answer`. */
   function controllerWith(
      answer: (options: DryRunOptions) => Promise<unknown>,
      { frozen = false } = {},
   ) {
      const seen: DryRunOptions[] = [];
      const environment = {
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
      const controller = new PackageController({
         publisherConfigIsFrozen: frozen,
         serverRootPath: os.tmpdir(),
         getEnvironment,
      } as unknown as EnvironmentStore);
      return { controller, seen, getEnvironment };
   }

   const loadedPackage = {
      getPackageMetadata: () => ({ name: "pkg", warnings: [] }),
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

   it("answers the package as it would load, with the location it came from, the version it would publish as, and no serving state", async () => {
      const { controller, seen } = controllerWith(async (options) =>
         options.describe(loadedPackage, "1.2.0"),
      );
      const answer = await controller.dryRunPackage("env", body);
      expect(answer).toEqual({
         name: "pkg",
         warnings: [],
         location: "gs://bucket/pkg.zip",
         versionId: "1.2.0",
      });
      expect(seen).toHaveLength(1);
   });

   it("refuses the request fields a publish refuses, before loading anything", async () => {
      const { controller, seen } = controllerWith(async (options) =>
         options.describe(loadedPackage, "1.2.0"),
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

   describe("over a real environment", () => {
      // The version is read by the environment from the tree the download
      // writes, so this is the one place a dry run can be seen reading it.
      let root: string;
      let env: Environment;
      let controller: PackageController;

      const tree = (name: string, manifest: Record<string, unknown>) => {
         const dir = path.join(root, "sources", name);
         fs.mkdirSync(dir, { recursive: true });
         fs.writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify(manifest),
         );
         fs.writeFileSync(
            path.join(dir, "model.malloy"),
            'source: numbers is duckdb.sql("SELECT 1 AS answer")\n',
         );
         return dir;
      };

      beforeEach(async () => {
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "package-dry-run-")),
         );
         const envPath = path.join(root, "env");
         fs.mkdirSync(envPath);
         env = await Environment.create("env", envPath, []);
         controller = new PackageController({
            publisherConfigIsFrozen: false,
            serverRootPath: root,
            getEnvironment: async () => env,
            downloadPackageInto: async (
               _environmentName: string,
               _packageName: string,
               location: string,
               stagingPath: string,
            ) => {
               await fs.promises.cp(location, stagingPath, { recursive: true });
            },
         } as unknown as EnvironmentStore);
      });

      afterEach(async () => {
         await env.closeAllConnections();
         fs.rmSync(root, { recursive: true, force: true });
      });

      it("reads the version even from a tree that declares none, refusing it with the publish's 400 MANIFEST_VERSION_MISSING", async () => {
         const refused = await controller
            .dryRunPackage("env", {
               name: "unversioned",
               location: tree("unversioned", { name: "unversioned" }),
            })
            .then(
               () => undefined,
               (err: unknown) => err,
            );
         expect(refused).toBeInstanceOf(PackageVersionError);
         expect(
            internalErrorToHttpError(refused as Error, { log: false }),
         ).toMatchObject({
            status: 400,
            json: { reason: "MANIFEST_VERSION_MISSING" },
         });
         expect(await env.listPackages()).toEqual([]);
      });

      it("names the version a tree that declares one would publish as", async () => {
         // So the 400 above is the missing version, not the tree.
         const location = tree("versioned", {
            name: "versioned",
            version: "1.2.0",
         });
         const answer = await controller.dryRunPackage("env", {
            name: "versioned",
            location,
         });
         expect(answer).toMatchObject({
            name: "versioned",
            location,
            versionId: "1.2.0",
         });
         expect(await env.listPackages()).toEqual([]);
      });
   });
});
