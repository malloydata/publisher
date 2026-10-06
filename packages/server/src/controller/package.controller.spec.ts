// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import sinon from "sinon";

import type { components } from "../api";
import { BadRequestError } from "../errors";
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
      const environment = { addPackage, unloadPackage, deletePackage };
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
            explores: ["missing.malloy"],
         }),
      ).rejects.toBeInstanceOf(BadRequestError);

      expect(unloadPackage.calledOnceWith("pkg")).toBe(true);
      expect(deletePackage.called).toBe(false);
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
      const environment = { installPackage, unloadPackage, deletePackage };
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
      const getEnvironment = sinon.stub().resolves({ addPackage });
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = {
         publisherConfigIsFrozen: false,
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
      const environment = { addPackage, unloadPackage };
      const getEnvironment = sinon.stub().resolves(environment);
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = {
         publisherConfigIsFrozen: false,
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
      const environment = { installPackage };
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
      const getEnvironment = sinon
         .stub()
         .resolves({ addPackage, unloadPackage });
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = {
         publisherConfigIsFrozen: false,
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
      const getEnvironment = sinon.stub().resolves({ addPackage });
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = {
         publisherConfigIsFrozen: false,
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
         peekPackage: () => undefined,
         installingFrom: () => undefined,
         installPackage,
         updatePackage,
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
      const getEnvironment = sinon.stub().resolves(environment);
      const addPackageToDatabase = sinon.stub().resolves(undefined);
      const environmentStore = {
         publisherConfigIsFrozen: false,
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
         installingFrom: () => undefined,
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
         installingFrom: () => undefined,
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
      expect(installPackage.firstCall.args[3]).toEqual({
         location: "gs://bucket/pkg___1.0.1.zip",
         update: body,
      });
      expect(updatePackage.called).toBe(false);
   });

   it("a PATCH matching the location of an install in flight is a metadata update, not a second install", async () => {
      const installPackage = sinon.stub().resolves(servedPackage);
      const updatePackage = sinon.stub().resolves({ name: "pkg" });
      const { controller } = controllerWith({
         peekPackage: () => undefined,
         installingFrom: () => "gs://bucket/pkg___1.0.0.zip",
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

   it("a PATCH on a package not loaded here installs it", async () => {
      const installPackage = sinon.stub().resolves(servedPackage);
      const updatePackage = sinon.stub().resolves({ name: "pkg" });
      const { controller } = controllerWith({
         peekPackage: () => undefined,
         installingFrom: () => undefined,
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

describe("PackageController.addPackage manifestLocation", () => {
   afterEach(() => {
      sinon.restore();
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
      const environment = { installPackage };
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
         location: "gs://bucket/pkg___1.0.0.zip",
         update: {
            location: "gs://bucket/pkg___1.0.0.zip",
            manifestLocation: "gs://bucket/pkg___1.0.0.manifest.json",
         },
      });
   });
});
