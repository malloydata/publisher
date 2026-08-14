import { afterEach, describe, expect, it } from "bun:test";
import sinon from "sinon";

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
      const environment = { installPackage, updatePackage };
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
