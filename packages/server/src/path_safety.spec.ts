import { describe, expect, it } from "bun:test";
import * as path from "path";

import { BadRequestError } from "./errors";
import {
   assertSafeEnvironmentPath,
   assertSafePackageName,
   assertSafeRelativeModelPath,
   safeJoinUnderRoot,
} from "./path_safety";

describe("assertSafePackageName", () => {
   it.each([
      "pkg",
      "test_package",
      "test-package",
      "TestPackage1",
      "test.package.name",
      "a",
      "x".repeat(255),
   ])("accepts %p", (name) => {
      expect(() => assertSafePackageName(name)).not.toThrow();
   });

   it.each([
      ["empty", ""],
      ["dot", "."],
      ["dot-dot", ".."],
      ["leading dot", ".staging"],
      ["forward slash", "foo/bar"],
      ["backslash", "foo\\bar"],
      ["null byte", "foo\0bar"],
      ["traversal", "../etc/passwd"],
      ["abs", "/etc/passwd"],
      ["space", "my pkg"],
      ["unicode", "pkg\u202E"],
      ["too long", "x".repeat(256)],
   ])("rejects %s (%p)", (_label, name) => {
      expect(() => assertSafePackageName(name)).toThrow(BadRequestError);
   });

   it.each([
      ["number", 42],
      ["null", null],
      ["undefined", undefined],
      ["object", { name: "pkg" }],
   ])("rejects non-string %s (%p)", (_label, value) => {
      expect(() => assertSafePackageName(value)).toThrow(BadRequestError);
   });
});

describe("assertSafeRelativeModelPath", () => {
   it.each([
      "model.malloy",
      "models/foo.malloy",
      "a/b/c/d.malloynb",
      "deep/nested/file_name-1.malloy",
   ])("accepts %p", (modelPath) => {
      expect(() => assertSafeRelativeModelPath(modelPath)).not.toThrow();
   });

   it.each([
      ["empty", ""],
      ["leading slash (absolute)", "/etc/passwd"],
      ["traversal", "../etc/passwd"],
      ["embedded traversal", "models/../../../etc/passwd"],
      ["embedded dot segment", "models/./foo.malloy"],
      ["double slash", "models//foo.malloy"],
      ["trailing slash", "models/foo/"],
      ["backslash", "models\\foo.malloy"],
      ["null byte", "models/foo\0.malloy"],
      ["dotfile segment", ".staging/foo.malloy"],
      ["dotfile leaf", "models/.hidden.malloy"],
   ])("rejects %s (%p)", (_label, modelPath) => {
      expect(() => assertSafeRelativeModelPath(modelPath)).toThrow(
         BadRequestError,
      );
   });

   it("rejects non-string inputs", () => {
      expect(() => assertSafeRelativeModelPath(undefined)).toThrow(
         BadRequestError,
      );
      expect(() => assertSafeRelativeModelPath(123)).toThrow(BadRequestError);
   });
});

describe("assertSafeEnvironmentPath", () => {
   it.each([
      "/etc/publisher",
      "/var/lib/publisher/env1",
      "/Users/me/data",
      "/a",
      "C:\\Users\\me\\publisher",
      "C:/Users/me/publisher",
   ])("accepts %p", (p) => {
      expect(() => assertSafeEnvironmentPath(p)).not.toThrow();
   });

   it.each([
      ["empty", ""],
      ["relative", "publisher/data"],
      ["traversal in middle", "/var/lib/../../etc/passwd"],
      ["traversal at end", "/var/lib/publisher/.."],
      ["null byte", "/var/lib/publisher\0"],
      ["bare dot-dot", ".."],
      ["bare dot", "."],
      ["too long", "/" + "a".repeat(5000)],
   ])("rejects %s (%p)", (_label, p) => {
      expect(() => assertSafeEnvironmentPath(p)).toThrow(BadRequestError);
   });

   it("rejects non-string inputs", () => {
      expect(() => assertSafeEnvironmentPath(undefined)).toThrow(
         BadRequestError,
      );
      expect(() => assertSafeEnvironmentPath(null)).toThrow(BadRequestError);
      expect(() => assertSafeEnvironmentPath(42)).toThrow(BadRequestError);
   });
});

describe("safeJoinUnderRoot", () => {
   const root = "/tmp/test-root";

   it("returns the resolved root when joined with no segments", () => {
      expect(safeJoinUnderRoot(root)).toBe(path.resolve(root));
   });

   it("joins safe segments into a path under root", () => {
      expect(safeJoinUnderRoot(root, "pkg", "model.malloy")).toBe(
         path.resolve(root, "pkg", "model.malloy"),
      );
   });

   it("throws when traversal escapes the root", () => {
      expect(() => safeJoinUnderRoot(root, "..")).toThrow(BadRequestError);
      expect(() => safeJoinUnderRoot(root, "..", "etc", "passwd")).toThrow(
         BadRequestError,
      );
      expect(() => safeJoinUnderRoot(root, "pkg", "..", "..", "etc")).toThrow(
         BadRequestError,
      );
   });

   it("throws when an absolute segment overrides the root", () => {
      expect(() => safeJoinUnderRoot(root, "/etc/passwd")).toThrow(
         BadRequestError,
      );
   });

   it("does NOT match a sibling directory with the same prefix", () => {
      // path.resolve("/tmp/test-root", "../test-root-bad")  ->  "/tmp/test-root-bad"
      // which starts with "/tmp/test-root" textually but is NOT a child.
      expect(() => safeJoinUnderRoot(root, "..", "test-root-bad")).toThrow(
         BadRequestError,
      );
   });
});
