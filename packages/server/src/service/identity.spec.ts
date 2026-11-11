// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { BadRequestError, PackageVersionError } from "../errors";
import {
   parseEnvironmentName,
   parseModelPath,
   parsePackageName,
   parsePackageRef,
   tryParseModelPath,
   VersionRef,
} from "./identity";

describe("identity names", () => {
   it("parse the names the path checks admit, unchanged", () => {
      expect(parseEnvironmentName("prod") as string).toBe("prod");
      expect(parsePackageName("sales-1.2") as string).toBe("sales-1.2");
      expect(parseModelPath("models/orders.malloy") as string).toBe(
         "models/orders.malloy",
      );
   });

   it("refuse what they refuse, with the same error and message", () => {
      expect(() => parseEnvironmentName("../etc")).toThrow(
         'Invalid package name: must be 1-255 characters of letters, digits, "-", "_", or "." and must not start with "."',
      );
      expect(() => parsePackageName(".staging")).toThrow(BadRequestError);
      expect(() => parsePackageName(["a"])).toThrow(BadRequestError);
      expect(() => parseModelPath("../x.malloy")).toThrow("Invalid model path");
   });

   it("tryParseModelPath answers undefined for a path no package serves", () => {
      expect(tryParseModelPath("a/../b.malloy")).toBeUndefined();
      expect(tryParseModelPath(".hidden.malloy")).toBeUndefined();
      expect(tryParseModelPath("m.malloy") as string).toBe("m.malloy");
   });

   it("parsePackageRef checks the environment first", () => {
      expect(() =>
         parsePackageRef({ environmentName: "..", packageName: ".." }),
      ).toThrow(BadRequestError);
      const ref = parsePackageRef({
         environmentName: "env",
         packageName: "pkg",
         versionId: "1.0.0",
      });
      expect(ref.environment as string).toBe("env");
      expect(ref.package as string).toBe("pkg");
      expect(ref.version.requested()).toBe("1.0.0");
   });
});

describe("VersionRef", () => {
   it("reads absent, null, empty and `latest` as the package's latest", () => {
      for (const raw of [undefined, null, "", "latest"]) {
         const ref = VersionRef.parse(raw);
         expect(ref).toBe(VersionRef.LATEST);
         expect(ref.isLatest).toBe(true);
         expect(ref.requested()).toBeUndefined();
      }
   });

   it("names a semantic version", () => {
      const ref = VersionRef.parse("1.2.0-rc.1");
      expect(ref.isLatest).toBe(false);
      expect(ref.requested()).toBe("1.2.0-rc.1");
      expect(String(ref)).toBe("1.2.0-rc.1");
   });

   it("carries a malformed value and refuses it only when it is read", () => {
      for (const raw of ["two", ["1.0.0"], { kind: "version" }]) {
         const ref = VersionRef.parse(raw);
         expect(ref.isLatest).toBe(false);
         let thrown: unknown;
         try {
            ref.requested();
         } catch (error) {
            thrown = error;
         }
         expect(thrown).toBeInstanceOf(PackageVersionError);
         expect((thrown as PackageVersionError).reason).toBe(
            "VERSION_ID_INVALID",
         );
      }
   });

   it("parse is idempotent, and `of` refuses a malformed id at once", () => {
      const ref = VersionRef.parse("2.0.0");
      expect(VersionRef.parse(ref)).toBe(ref);
      expect(() => VersionRef.of("nope")).toThrow(PackageVersionError);
   });
});
