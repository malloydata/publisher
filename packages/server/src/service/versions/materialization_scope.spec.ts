// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { Materialization } from "../../storage/DatabaseInterface";
import {
   newestServingEntries,
   ownedVersionOf,
   REBUILDING_TABLES_KEY,
   versionedTableName,
   versionTableSuffix,
} from "./materialization_scope";

function run(
   id: string,
   overrides: Partial<Materialization> & { tables?: string[] } = {},
): Materialization {
   const { tables, ...rest } = overrides;
   return {
      id,
      environmentId: "env",
      packageName: "pkg",
      status: "MANIFEST_FILE_READY",
      manifest: {
         strict: false,
         entries: Object.fromEntries(
            (tables ?? [`t_${id}`]).map((table) => [
               `eid-${table}`,
               { sourceEntityId: `eid-${table}`, physicalTableName: table },
            ]),
         ),
      } as Materialization["manifest"],
      startedAt: null,
      completedAt: null,
      error: null,
      metadata: null,
      version: null,
      createdAt: new Date(),
      updatedAt: new Date(),
      ...rest,
   };
}

const tablesOf = (entries: Record<string, { physicalTableName: string }>) =>
   Object.values(entries).map((e) => e.physicalTableName);

describe("versionTableSuffix", () => {
   it("names a release by its numbers", () => {
      expect(versionTableSuffix("1.2.3")).toBe("__v1_2_3");
   });

   it("keeps versions apart that differ only in punctuation", () => {
      const a = versionTableSuffix("1.0.0-rc.1");
      const b = versionTableSuffix("1.0.0-rc-1");
      expect(a).toMatch(/^__v1_0_0_[0-9a-f]{8}$/);
      expect(a).not.toBe(b);
      expect(versionTableSuffix("1.0.0+build.5")).not.toBe(
         versionTableSuffix("1.0.0"),
      );
   });
});

describe("versionedTableName", () => {
   it("suffixes the table segment of a qualified name", () => {
      expect(versionedTableName("analytics.summary", "__v1_0_0")).toBe(
         "analytics.summary__v1_0_0",
      );
      expect(versionedTableName("summary", "__v1_0_0")).toBe("summary__v1_0_0");
   });

   it("keeps a long segment within 50 characters, and distinct names distinct", () => {
      const long = "x".repeat(60);
      const a = versionedTableName(`s.${long}a`, "__v1_0_0");
      const b = versionedTableName(`s.${long}b`, "__v1_0_0");
      expect(a.split(".")[1].length).toBeLessThanOrEqual(50);
      expect(a.endsWith("__v1_0_0")).toBe(true);
      expect(a).not.toBe(b);
   });
});

describe("ownedVersionOf", () => {
   const pkg = (versionId: string | undefined, scope?: string) => ({
      getVersionId: () => versionId,
      getPackageMetadata: () => ({ scope }),
   });

   it("is the version under scope version, and nothing otherwise", () => {
      expect(ownedVersionOf(pkg("1.0.0", "version"))).toBe("1.0.0");
      expect(ownedVersionOf(pkg("1.0.0", "package"))).toBeUndefined();
      expect(ownedVersionOf(pkg("1.0.0"))).toBeUndefined();
      expect(ownedVersionOf(pkg(undefined, "version"))).toBeUndefined();
   });
});

describe("newestServingEntries", () => {
   const owned = (id: string, version: string) =>
      run(id, { version, metadata: { scope: "version" } });

   it("reads a version's own runs only", () => {
      const runs = [owned("b", "2.0.0"), owned("a", "1.0.0"), run("legacy")];
      expect(tablesOf(newestServingEntries(runs, "1.0.0"))).toEqual(["t_a"]);
   });

   it("never reads a version-owned run as one of the package's", () => {
      const runs = [owned("b", "2.0.0"), run("shared", { version: "1.0.0" })];
      expect(tablesOf(newestServingEntries(runs, undefined))).toEqual([
         "t_shared",
      ]);
   });

   it("skips the run asking, and runs that did not commit", () => {
      const runs = [
         run("self"),
         run("failed", { status: "FAILED" }),
         run("committed"),
      ];
      expect(tablesOf(newestServingEntries(runs, undefined, "self"))).toEqual([
         "t_committed",
      ]);
   });

   it("drops the shared tables a newer run set out to rebuild without committing", () => {
      const runs = [
         run("failed", {
            status: "FAILED",
            metadata: { [REBUILDING_TABLES_KEY]: ["orders"] },
         }),
         run("committed", { tables: ["orders", "items"] }),
      ];
      expect(tablesOf(newestServingEntries(runs, undefined))).toEqual([
         "items",
      ]);
   });

   it("answers nothing when no run committed", () => {
      expect(
         newestServingEntries([run("x", { status: "PENDING" })], undefined),
      ).toEqual({});
   });
});
