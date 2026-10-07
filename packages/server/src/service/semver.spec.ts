// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { parse } from "yaml";
import {
   compareSemver,
   isSemver,
   SEMVER_PATTERN,
   versionDirName,
} from "./semver";

describe("isSemver", () => {
   const accepted = [
      "0.0.1",
      "1.2.3",
      "10.20.30",
      "1.0.0-rc1",
      "1.0.0-alpha.2",
      "1.0.0-0.3.7",
      "1.0.0+20231120.deadbeef",
      "1.0.0-rc.1+build.5",
      "1.0.0-x-y-z.--",
   ];
   for (const version of accepted) {
      it(`accepts ${version}`, () => expect(isSemver(version)).toBe(true));
   }

   const refused = [
      "",
      "1",
      "1.2",
      "v1.2.3",
      "1.2.3.4",
      "1.2.3-",
      "1.2.3+",
      "1.2.3-rc_1",
      "1.2.3 ",
      "latest",
      "../1.0.0",
   ];
   for (const version of refused) {
      it(`refuses ${JSON.stringify(version)}`, () =>
         expect(isSemver(version)).toBe(false));
   }

   it("is the same pattern the spec declares as VersionIdPattern", () => {
      const spec = parse(
         fs.readFileSync(
            path.join(__dirname, "..", "..", "..", "..", "api-doc.yaml"),
            "utf8",
         ),
      );
      expect(spec.components.schemas.VersionIdPattern.pattern).toBe(
         SEMVER_PATTERN.source,
      );
   });
});

describe("compareSemver", () => {
   // Ascending by SemVer 2.0 precedence; the spec's own worked example.
   const ascending = [
      "1.0.0-alpha",
      "1.0.0-alpha.1",
      "1.0.0-alpha.beta",
      "1.0.0-beta",
      "1.0.0-beta.2",
      "1.0.0-beta.11",
      "1.0.0-rc.1",
      "1.0.0",
      "1.0.1",
      "1.1.0",
      "2.0.0",
      "10.0.0",
   ];

   it("orders versions by precedence, not as text", () => {
      const shuffled = [...ascending].reverse();
      expect(shuffled.sort(compareSemver)).toEqual(ascending);
   });

   it("puts a pre-release below its release", () => {
      expect(compareSemver("1.0.0-rc1", "1.0.0")).toBeLessThan(0);
      expect(compareSemver("1.0.0", "1.0.0-rc1")).toBeGreaterThan(0);
   });

   it("ignores build metadata", () => {
      expect(compareSemver("1.0.0+a", "1.0.0+b")).toBe(0);
      expect(compareSemver("1.0.0+a", "1.0.0")).toBe(0);
   });

   it("compares components larger than a JavaScript number holds exactly", () => {
      expect(
         compareSemver("9007199254740993.0.0", "9007199254740992.0.0"),
      ).toBeGreaterThan(0);
   });

   it("refuses a value that is not a version", () => {
      expect(() => compareSemver("latest", "1.0.0")).toThrow(
         "Not a semantic version",
      );
   });
});

describe("versionDirName", () => {
   it("is the version itself when it carries no build metadata", () => {
      expect(versionDirName("1.0.0-rc.1")).toBe("1.0.0-rc.1");
   });

   it("maps build metadata's + to _, which no version contains", () => {
      expect(versionDirName("1.0.0+build.5")).toBe("1.0.0_build.5");
   });

   it("refuses a value that is not a version, so no path is built from one", () => {
      expect(() => versionDirName("../../etc")).toThrow(
         "Not a semantic version",
      );
   });
});
