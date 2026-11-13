// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover's conformance runner as a library (6e, plan question 15):
 * a package and its queries in, one classified row per query out. Run here
 * over the fixture's shapes, as Credible's private CI runs it over real
 * package snapshots.
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { runV1CutoverConformance } from "../../../src/test_helpers/conformance/v1_cutover_runner";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import { DATA_FILES } from "./conformance/data";
import { renderFiles, SHAPES } from "./conformance/shapes";

describe("runV1CutoverConformance", () => {
   it("classifies each query live and served, from a package directory", async () => {
      const shape = SHAPES.find((s) => s.name === "baseline")!;
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "v1-cutover-runner-"));
      try {
         writePackageFiles(dir, {
            ...DATA_FILES,
            ...renderFiles(shape, "colocated"),
         });
         const rows = await runV1CutoverConformance({
            package: { packageDir: dir },
            queries: shape.queries.map((q) => ({
               name: q.name,
               model: q.model ?? "m.malloy",
               malloy: q.malloy,
            })),
         });
         expect(rows.map((r) => [r.query, r.verdict, r.cutoverPath])).toEqual(
            shape.queries.map((q) => [q.name, "served_equal", "served_copy"]),
         );
      } finally {
         fs.rmSync(dir, { recursive: true, force: true });
      }
   }, 300_000);

   it("reports the path that answers, from the package's files", async () => {
      const shape = SHAPES.find(
         (s) => s.name === "v1 rollup on annotated measures",
      )!;
      const rows = await runV1CutoverConformance({
         package: { ...DATA_FILES, ...renderFiles(shape, "storage") },
         queries: shape.queries.map((q) => ({
            name: q.name,
            model: "m.malloy",
            malloy: q.malloy,
         })),
      });
      const byName = Object.fromEntries(rows.map((r) => [r.query, r]));
      // The rollup leg answers, with the measures' annotations.
      expect(byName["covered by the rollup"]).toMatchObject({
         cutover: "served",
         cutoverPath: "rollup",
      });
      expect(byName["not covered (status)"]).toMatchObject({
         verdict: "live",
      });
   }, 300_000);
});
