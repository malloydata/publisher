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
      const mockPackage = { formatInvalidExplores: () => invalidMsg };
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
