// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One tier word on the wire: a table in the source's own connection is
 * `database`, wherever a tier is produced. `colocated`, the word releases
 * before this one produced, stays an accepted value of the API enums that
 * carried it, so a client or a stored record holding it still validates.
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { BUILD_TIERS } from "../../materialization_metrics";
import { tierOf } from "./plan/refusal_key";
import {
   DEPRECATED_PREAGGREGATE_ANNOTATION_CODE,
   ROLLUP_NAMESPACE_DEPRECATED_CODE,
   ROLLUP_INVALID_CODE,
   ROLLUP_NOT_BUILT_CODE,
} from "../annotation_reader";
import { isRollupOrigin, originOf, originOfTable } from "./domain/table_plans";

// Line endings normalized: a Windows checkout reads the file with CRLF.
const API_DOC = fs
   .readFileSync(path.join(__dirname, "../../../../../api-doc.yaml"), "utf8")
   .replace(/\r\n/g, "\n");

/** The `enum:` line of `property` inside `schema`, read off api-doc.yaml. */
function enumOf(schema: string, property: string): string[] {
   const start = API_DOC.indexOf(`\n    ${schema}:\n`);
   expect(start).toBeGreaterThan(-1);
   const body = API_DOC.slice(start + 1);
   const end = body.search(/\n {4}[A-Za-z]+:\n/);
   const block = end > 0 ? body.slice(0, end) : body;
   const at = block.indexOf(`\n        ${property}:\n`);
   expect(at).toBeGreaterThan(-1);
   const line = /enum: \[([^\]]*)\]/.exec(block.slice(at))?.[1] ?? "";
   return line.split(",").map((v) => v.trim());
}

describe("the tier vocabulary", () => {
   it("produces database, never colocated", () => {
      expect(tierOf(undefined)).toBe("database");
      expect(tierOf("lake")).toBe("storage");
      expect([...BUILD_TIERS]).toEqual(["database", "storage"]);
   });

   it("keeps colocated an accepted value of RefusedSource.tier", () => {
      expect(enumOf("RefusedSource", "tier")).toEqual([
         "storage",
         "database",
         "preaggregate",
         "colocated",
      ]);
   });

   it("documents colocated as retained on ServeVerdict.tier", () => {
      const start = API_DOC.indexOf("\n    ServeVerdict:\n");
      const block = API_DOC.slice(start, start + 4000);
      expect(block).toMatch(/`database`[\s\S]*`colocated` is\s+retained/);
   });
});

describe("the rollup vocabulary", () => {
   it("produces rollup as a rollup's origin, and reads preaggregate as one", () => {
      expect(originOfTable("rollup", "r@file:///m.malloy")).toBe("rollup");
      expect(originOf("preaggregate")).toBe("rollup");
      expect(originOf(undefined)).toBe("persist");
      expect(isRollupOrigin("preaggregate")).toBe(true);
      expect(isRollupOrigin("rollup")).toBe(true);
      expect(isRollupOrigin("persist")).toBe(false);
   });

   it("names its codes for #(rollup), the old spelling only on the warning about it", () => {
      expect(ROLLUP_NOT_BUILT_CODE).toBe("rollup-not-built");
      expect(ROLLUP_INVALID_CODE).toBe("rollup-invalid");
      expect(DEPRECATED_PREAGGREGATE_ANNOTATION_CODE).toBe(
         "deprecated-preaggregate-annotation",
      );
      expect(ROLLUP_NAMESPACE_DEPRECATED_CODE).toBe(
         "deprecated-rollup-namespace",
      );
   });

   it("keeps preaggregate an accepted origin where releases wrote it, and produces none on new fields", () => {
      for (const schema of [
         "PersistSourcePlan",
         "ManifestEntry",
         "StorageServeBinding",
      ]) {
         expect(enumOf(schema, "origin")).toEqual([
            "persist",
            "materialize",
            "view",
            "query",
            "rollup",
            "preaggregate",
         ]);
      }
      for (const schema of [
         "RefusedSource",
         "QueryPathServer",
         "QueryPathCandidate",
         "QueryPathNearby",
      ]) {
         expect(enumOf(schema, "origin")).toEqual([
            "persist",
            "materialize",
            "view",
            "query",
            "rollup",
         ]);
      }
   });

   it("lists each renamed code as retained", () => {
      for (const code of [
         "preaggregate-invalid",
         "preaggregate-unknown-key",
         "preaggregate-unparseable",
         "preaggregate-not-built",
         "preaggregate-not-a-measure",
         "preaggregate-unknown-grain",
         "preaggregate-not-mergeable",
         "preaggregate-join-one-key-repeated",
      ]) {
         expect(API_DOC).toContain(`\`${code}\``);
      }
      // The warning's old code is repeated beside the new one this release.
      expect(API_DOC).toMatch(
         /`preaggregate-not-built` is\s+`rollup-not-built`'s former name/,
      );
      for (const code of [
         "rollup-not-a-measure",
         "rollup-unknown-grain",
         "rollup-not-mergeable",
         "rollup-join-one-key-repeated",
      ]) {
         expect(API_DOC).toContain(`\`${code}\``);
      }
   });
});
