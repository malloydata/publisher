// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import sinon from "sinon";

import type { components } from "../api";
import {
   BadRequestError,
   PackageAdmissionRefusedError,
   PackageVersionError,
   ServiceUnavailableError,
} from "../errors";
import { logger } from "../logger";
import type { EnvironmentStore } from "../service/environment_store";
import { PackageController } from "./package.controller";

describe("PackageController.addPackage explores validation", () => {
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
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
         addPackageToDatabase,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);

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
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
         addPackageToDatabase,
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

      expect(versions.publishStagedVersion.calledOnce).toBe(true);
      expect(
         typeof versions.publishStagedVersion.firstCall.args[1].validate,
      ).toBe("function");
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
      const environmentStore = {
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
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
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const addPackage = sinon.stub().resolves(mockPackage);
      const getEnvironment = sinon
         .stub()
         .resolves({ addPackage, getVersionService: () => null });
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
         addPackageToDatabase,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);

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
      // Valid explores but an invalid persistence policy (e.g. a schedule on a
      // package-scoped package): the publish must still 400 (strict-at-publish,
      // same split as explores — load merely warns) and roll back via
      // unloadPackage.
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
      const unloadPackage = sinon.stub().resolves(undefined);
      const addPackage = sinon.stub().resolves(mockPackage);
      const environment = {
         getVersionService: () => null,
         addPackage,
         unloadPackage,
      };
      const getEnvironment = sinon.stub().resolves(environment);
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
         addPackageToDatabase,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);

      await expect(
         controller.addPackage("env", { name: "pkg", description: "test" }),
      ).rejects.toThrow(cronMsg);

      expect(unloadPackage.calledOnceWith("pkg")).toBe(true);
      expect(addPackageToDatabase.called).toBe(false);
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
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
         addPackageToDatabase,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);

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
      const environmentStore = {
         publisherConfigIsFrozen: false,
         getEnvironment,
         addPackageToDatabase,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);

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
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
         addPackageToDatabase,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);

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
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      };
      const addPackage = sinon.stub().resolves(mockPackage);
      const getEnvironment = sinon
         .stub()
         .resolves({ addPackage, getVersionService: () => null });
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
         addPackageToDatabase,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);
      await controller.addPackage("env", { name: "pkg", description: "test" });

      expect(addPackageToDatabase.calledOnceWith("env", "pkg")).toBe(true);
   });
});

describe("PackageController.updatePackage explores validation", () => {
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
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
         addPackageToDatabase,
      } as unknown as EnvironmentStore;

      const controller = new PackageController(environmentStore);

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
      const _package = { getPackageMetadata: () => metadata };
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
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment,
         addPackageToDatabase,
      } as unknown as EnvironmentStore;
      return {
         controller: new PackageController(environmentStore),
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
         description: "next",
      };

      await controller.updatePackage("env", "pkg", body);

      expect(installPackage.calledOnce).toBe(true);
      // The metadata rides with the install so both land under one lock hold;
      // a separate update call is exactly the second lock acquisition that a
      // queued delete could run between.
      expect(installPackage.firstCall.args[3]).toEqual({ update: body });
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
         description: "renamed",
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
      };
      const getPackage = sinon.stub().resolves(cached);
      const installPackage = sinon.stub().resolves(reinstalled);
      const environmentStore = {
         getEnvironment: sinon.stub().resolves({
            getVersionService: () => null,
            getPackage,
            installPackage,
         }),
      } as unknown as EnvironmentStore;
      const controller = new PackageController(environmentStore);
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
         addPackageToDatabase: sinon.stub().resolves(undefined),
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
      expect(versions.stageForPublish.firstCall.args[0]).toBe("pkg");
      expect(versions.publishStagedVersion.firstCall.args[1]).toMatchObject({
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
      expect(
         versions.publishStagedVersion.secondCall.args[1].manifestLocation,
      ).toBeNull();
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
         formatInvalidIncrementalPolicy: () => "",
         formatInvalidPreaggregatePolicy: () => "",
         formatPersistenceCollisionRejections: () => "",
      });
      const environment = { getVersionService: () => null, installPackage };
      const environmentStore = {
         publisherConfigIsFrozen: false,
         getEnvironment: sinon.stub().resolves(environment),
         addPackageToDatabase: sinon.stub().resolves(undefined),
      } as unknown as EnvironmentStore;
      const controller = new PackageController(environmentStore);

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

describe("PackageController.addPackage with a tree that declares no semantic version", () => {
   const installedPackage = {
      formatInvalidExplores: () => "",
      formatInvalidPersistencePolicy: () => "",
      formatInvalidIncrementalPolicy: () => "",
      formatInvalidPreaggregatePolicy: () => "",
      formatPersistenceCollisionRejections: () => "",
   };
   let stagingRoot: string;

   afterEach(() => {
      sinon.restore();
      fs.rmSync(stagingRoot, { recursive: true, force: true });
   });

   /**
    * A version service whose download stages a real tree that declares no
    * semantic version (`reason`), for a package with or without versions.
    */
   function unversionedStaging(
      reason: PackageVersionError,
      opts: { hasVersions?: boolean; hasManifest?: boolean } = {},
   ) {
      stagingRoot = fs.mkdtempSync(path.join(os.tmpdir(), "unversioned-"));
      const stagingPath = path.join(stagingRoot, "staged");
      fs.mkdirSync(stagingPath);
      fs.writeFileSync(
         path.join(stagingPath, "model.malloy"),
         "source: s is x",
      );
      return {
         isVersioned: async () => opts.hasVersions ?? false,
         stageForPublish: sinon.stub().resolves({
            packageName: "pkg",
            stagingPath,
            reason,
            hasManifest: opts.hasManifest ?? true,
         }),
         discardStage: sinon.stub().resolves(undefined),
         publishStagedVersion: sinon.stub().rejects(new Error("not a version")),
      };
   }

   function controllerWith(versions: object, installPackage: sinon.SinonStub) {
      const environmentStore = {
         publisherConfigIsFrozen: false,
         serverRootPath: os.tmpdir(),
         getEnvironment: sinon.stub().resolves({
            getVersionService: () => versions,
            installPackage,
         }),
         addPackageToDatabase: sinon.stub().resolves(undefined),
      } as unknown as EnvironmentStore;
      return new PackageController(environmentStore);
   }

   it("installs it in place as before, from the one download, and warns that versions exist", async () => {
      // Published as latest, replacing the package: what every publish from a
      // location did before versions, with the install's own arguments. The
      // tree the version check downloaded is the one installed.
      const versions = unversionedStaging(
         new PackageVersionError("MANIFEST_VERSION_MISSING", "no version"),
      );
      const installed: string[] = [];
      const installPackage = sinon
         .stub()
         .callsFake(
            async (
               _name: string,
               downloader: (stagingPath: string) => Promise<void>,
            ) => {
               const target = path.join(stagingRoot, "install");
               await downloader(target);
               installed.push(...fs.readdirSync(target));
               return installedPackage;
            },
         );
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      try {
         await controllerWith(versions, installPackage).addPackage("env", {
            name: "pkg",
            location: "gs://bucket/pkg.zip",
            manifestLocation: null,
         });

         expect(installPackage.calledOnce).toBe(true);
         expect(installPackage.firstCall.args[3]).toEqual({
            update: { location: "gs://bucket/pkg.zip" },
         });
         expect(installed).toEqual(["model.malloy"]);
         expect(versions.publishStagedVersion.called).toBe(false);
         expect(versions.discardStage.calledOnce).toBe(true);
         const warned = (warn.mock.calls as unknown as unknown[][]).filter(
            ([message]) =>
               String(message).includes(
                  "Publisher now supports package versioning",
               ),
         );
         expect(warned).toHaveLength(1);
         expect(String(warned[0][0])).toContain("pkg");
         expect(warned[0][1]).toEqual({
            packageName: "pkg",
            reason: "MANIFEST_VERSION_MISSING",
         });
      } finally {
         warn.mockRestore();
      }
   });

   it("refuses it for a package that has published versions, with what is missing", async () => {
      // A versioned package is never installed in place, so the answer is the
      // version the tree does not declare.
      const versions = unversionedStaging(
         new PackageVersionError("MANIFEST_VERSION_INVALID", "not semver"),
         { hasVersions: true },
      );
      const installPackage = sinon.stub().resolves(installedPackage);
      const refused = await controllerWith(versions, installPackage)
         .addPackage("env", { name: "pkg", location: "/srv/pkg" })
         .then(
            () => undefined,
            (err: unknown) => err,
         );
      expect(refused).toBeInstanceOf(PackageVersionError);
      expect((refused as PackageVersionError).reason).toBe(
         "MANIFEST_VERSION_INVALID",
      );
      expect(installPackage.called).toBe(false);
      expect(versions.discardStage.calledOnce).toBe(true);
   });

   it("hands a location with no publisher.json to the install without the versioning warning", async () => {
      // No publisher.json is no package: the install fails as it always has,
      // and a note about adding a version would point the wrong way.
      const versions = unversionedStaging(
         new PackageVersionError("MANIFEST_VERSION_MISSING", "no manifest"),
         { hasManifest: false },
      );
      const installPackage = sinon
         .stub()
         .rejects(new BadRequestError("Package manifest does not exist."));
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      try {
         await expect(
            controllerWith(versions, installPackage).addPackage("env", {
               name: "pkg",
               location: "/srv/missing",
            }),
         ).rejects.toBeInstanceOf(BadRequestError);
         expect(installPackage.calledOnce).toBe(true);
         expect(
            (warn.mock.calls as unknown as unknown[][]).some(([message]) =>
               String(message).includes("supports package versioning"),
            ),
         ).toBe(false);
         expect(versions.discardStage.calledOnce).toBe(true);
      } finally {
         warn.mockRestore();
      }
   });
});

/**
 * A version service whose staged tree declares version 1.0.0 (or whose
 * staging fails with `failWith`), and whose publish of it runs the publish
 * checks it is handed on `loaded`, as the real one does before it records
 * anything, and otherwise answers with it.
 */
function versionsPublishing(loaded: unknown, failWith?: Error) {
   const staged = {
      packageName: "pkg",
      stagingPath: "/staging/pkg",
      versionId: "1.0.0",
      dirName: "1.0.0",
      contentHash: "0",
      description: null,
   };
   return {
      isVersioned: async () => false,
      stageForPublish: sinon.stub().callsFake(async () => {
         if (failWith) throw failWith;
         return staged;
      }),
      discardStage: sinon.stub().resolves(undefined),
      publishStagedVersion: sinon
         .stub()
         .callsFake(
            async (
               _staged: unknown,
               options: { validate?: (pkg: unknown) => string | undefined },
            ) => {
               const msg = options.validate?.(loaded);
               if (msg) throw new BadRequestError(msg);
               return { loaded, version: {}, created: true };
            },
         ),
   };
}
