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

const API_DOC = fs.readFileSync(
   path.join(__dirname, "../../../../../api-doc.yaml"),
   "utf8",
);

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
