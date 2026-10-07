// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { internalErrorToHttpError, PackageVersionError } from "../errors";
import type { PackageVersion } from "../storage/DatabaseInterface";
import { Environment } from "./environment";

function version(
   packageName: string,
   v: string,
   overrides: Partial<PackageVersion> = {},
): PackageVersion {
   return {
      id: `${packageName}-${v}`,
      environmentId: "env-id",
      packageName,
      version: v,
      dirName: v.replace(/\+/g, "_"),
      contentHash: `hash-${v}`,
      sourceLocation: null,
      manifestLocation: null,
      archiveStatus: "unarchive",
      archivedAt: null,
      description: null,
      gitCommitSha: null,
      gitRef: null,
      createdAt: new Date("2026-10-01T00:00:00Z"),
      updatedAt: new Date("2026-10-01T00:00:00Z"),
      ...overrides,
   };
}

function refusal(fn: () => unknown): { status: number; reason?: string } {
   try {
      fn();
   } catch (error) {
      expect(error).toBeInstanceOf(PackageVersionError);
      const { status, json } = internalErrorToHttpError(error as Error);
      return { status, reason: (json as { reason?: string }).reason };
   }
   throw new Error("expected a refusal");
}

describe("Environment.resolveSlot", () => {
   let rootDir: string;
   let envPath: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-slots-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   describe("an unversioned package", () => {
      it("resolves to its single slot, keyed and placed exactly as before versions", () => {
         expect(env.resolveSlot("sales")).toEqual({
            name: "sales",
            key: "sales",
            path: path.join(path.resolve(envPath), "sales"),
         });
         expect(env.isVersionedPackage("sales")).toBe(false);
         expect(env.listPackageVersions("sales")).toEqual({
            latest: null,
            versions: [],
         });
      });

      it("refuses a named version with 404 VERSION_NOT_FOUND rather than serving its only tree", () => {
         expect(refusal(() => env.resolveSlot("sales", "1.0.0"))).toEqual({
            status: 404,
            reason: "VERSION_NOT_FOUND",
         });
      });
   });

   describe("a versioned package", () => {
      beforeEach(() => {
         env.setPackageVersions("sales", "1.1.0", [
            version("sales", "1.0.0"),
            version("sales", "1.1.0"),
            version("sales", "2.0.0-rc1+build.7"),
            version("sales", "0.9.0", { archiveStatus: "archive" }),
         ]);
      });

      it("resolves an omitted version to latest", () => {
         const slot = env.resolveSlot("sales");
         expect(slot.version?.version).toBe("1.1.0");
         expect(slot.key).toBe("sales@1.1.0");
         expect(slot.path).toBe(
            path.join(path.resolve(envPath), "sales", "1.1.0"),
         );
      });

      it("resolves a named version, under its directory name", () => {
         const slot = env.resolveSlot("sales", "2.0.0-rc1+build.7");
         expect(slot.key).toBe("sales@2.0.0-rc1_build.7");
         expect(slot.path).toBe(
            path.join(path.resolve(envPath), "sales", "2.0.0-rc1_build.7"),
         );
      });

      it("refuses an unknown version with 404 VERSION_NOT_FOUND", () => {
         expect(refusal(() => env.resolveSlot("sales", "3.0.0"))).toEqual({
            status: 404,
            reason: "VERSION_NOT_FOUND",
         });
      });

      it("refuses an archived version with 410 VERSION_ARCHIVED", () => {
         expect(refusal(() => env.resolveSlot("sales", "0.9.0"))).toEqual({
            status: 410,
            reason: "VERSION_ARCHIVED",
         });
      });

      it("refuses an omitted version when the package has no latest", () => {
         env.setPackageVersions("sales", null, [version("sales", "1.0.0")]);
         expect(refusal(() => env.resolveSlot("sales"))).toEqual({
            status: 404,
            reason: "VERSION_NOT_FOUND",
         });
         expect(env.resolveSlot("sales", "1.0.0").key).toBe("sales@1.0.0");
      });

      it("lists versions highest first, archived ones included", () => {
         expect(
            env.listPackageVersions("sales").versions.map((v) => v.version),
         ).toEqual(["2.0.0-rc1+build.7", "1.1.0", "1.0.0", "0.9.0"]);
         expect(env.listPackageVersions("sales").latest).toBe("1.1.0");
      });

      it("goes back to a single unversioned slot once its versions are cleared", () => {
         env.clearPackageVersions("sales");
         expect(env.resolveSlot("sales").key).toBe("sales");
      });
   });

   it("refuses a package name that is not a safe path segment", () => {
      expect(() => env.resolveSlot("../escape")).toThrow();
   });
});
