// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   isVersionSegment,
   packagePath,
   splitVersionSegment,
   versionIdOf,
   withVersionSegment,
} from "./packageRoute";

describe("package routes with a version segment", () => {
   it("reads latest and semantic versions as version segments", () => {
      for (const segment of [
         "latest",
         "1.2.0",
         "0.0.1",
         "1.0.0-rc.1",
         "1.0.0+build.7",
         "2.0.0-alpha.1+20261015",
      ]) {
         expect(isVersionSegment(segment)).toBe(true);
      }
   });

   it("does not read paths, files or near-versions as versions", () => {
      for (const segment of [
         "sales",
         "sales.malloy",
         "data-apps",
         "1.2",
         "v1.2.0",
         "01.2.0",
         "Latest",
         "",
      ]) {
         expect(isVersionSegment(segment)).toBe(false);
      }
   });

   it("splits the version segment from the rest of the path", () => {
      expect(splitVersionSegment("1.2.0")).toEqual({
         segment: "1.2.0",
         rest: "",
      });
      expect(splitVersionSegment("latest/sales/q3")).toEqual({
         segment: "latest",
         rest: "sales/q3",
      });
      expect(splitVersionSegment("sales/q3")).toBeUndefined();
   });

   it("sends no versionId for latest", () => {
      expect(versionIdOf("latest")).toBeUndefined();
      expect(versionIdOf(undefined)).toBeUndefined();
      expect(versionIdOf("1.0.0")).toBe("1.0.0");
   });

   it("encodes each segment of a package path", () => {
      expect(packagePath("my env", "sales")).toBe("/my%20env/sales");
      expect(packagePath("e", "p", "1.0.0+b.1")).toBe("/e/p/1.0.0%2Bb.1");
   });

   it("puts the version on screen into links inside the package", () => {
      expect(withVersionSegment("/e/p/sales/q3", "e", "p", "1.0.0")).toBe(
         "/e/p/1.0.0/sales/q3",
      );
      expect(
         withVersionSegment("/e/p/data-apps/index.html", "e", "p", "latest"),
      ).toBe("/e/p/latest/data-apps/index.html");
      expect(withVersionSegment("/e/p", "e", "p", "1.0.0")).toBe("/e/p/1.0.0");
   });

   it("leaves links that already carry a version, or point elsewhere, alone", () => {
      expect(withVersionSegment("/e/p/2.0.0/q3", "e", "p", "1.0.0")).toBe(
         "/e/p/2.0.0/q3",
      );
      expect(withVersionSegment("/e/other/q3", "e", "p", "1.0.0")).toBe(
         "/e/other/q3",
      );
      expect(withVersionSegment("/e/p/q3", "e", "p", undefined)).toBe(
         "/e/p/q3",
      );
   });
});
