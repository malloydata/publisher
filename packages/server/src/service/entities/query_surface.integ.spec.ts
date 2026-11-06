// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @fileoverview The cases in `__fixtures__/query_surface_cases.ts`, compiled as
 * one real package against in-process DuckDB: each (file, source) must carry
 * the NOT_QUERYABLE marker exactly where the fixture's `queryable` says.
 *
 * The fixture's `publisherRuns` column records what the query route answers.
 * The two columns differ only for a source in a served report that the route
 * runs (a re-import of a served declaration, or a source derived from one),
 * which the first test here holds to.
 */
import { afterAll, beforeAll, describe, spyOn, test } from "bun:test";
import assert from "node:assert";
import path from "path";
import type { ModelCompileResult } from "./internal_types";
import { writeWorkspace, type Workspace } from "./test_support";
import * as report from "../report";
import { ConnectionLookup } from "./test_support";
import { packageCompilationService } from "./test_support";
import { packageManagementService } from "./test_support";
import {
   QUERY_SURFACE_CASES,
   QUERY_SURFACE_FILES,
} from "./__fixtures__/query_surface_cases";

/** Write the fixture package and compile every `.malloy` file in it. */
async function compileFixturePackage(): Promise<{
   ws: Workspace;
   results: ModelCompileResult[];
}> {
   const ws = await writeWorkspace(
      Object.entries(QUERY_SURFACE_FILES).map(([relpath, contents]) => ({
         relpath,
         contents,
      })),
      "entities-query-surface-",
   );
   const packageFiles = Object.keys(QUERY_SURFACE_FILES).map(
      (relativePath) => ({
         packageRootPath: ws.dir,
         absolutePath: path.join(ws.dir, relativePath),
         relativePath,
      }),
   );
   const manifest = packageManagementService.readPackageManifest(packageFiles);
   const lookup = await ConnectionLookup.getInstance([], ws.dir);
   const results = await packageCompilationService.compilePackageModelFiles(
      {
         modelFiles: packageFiles.filter((f) =>
            f.relativePath.endsWith(".malloy"),
         ),
         manifest,
      },
      lookup,
   );
   return { ws, results };
}

test("the index differs from Publisher only on a report's sources that re-import or derive from a served source", () => {
   for (const c of QUERY_SURFACE_CASES) {
      assert.strictEqual(
         c.queryable,
         c.publisherRuns && !c.servedElsewhere,
         `${c.file}::${c.source}`,
      );
   }
   assert.ok(QUERY_SURFACE_CASES.some((c) => c.servedElsewhere));
});

describe("NOT_QUERYABLE matches the fixture", () => {
   let ws: Workspace;
   let results: ModelCompileResult[];

   beforeAll(async () => {
      ({ ws, results } = await compileFixturePackage());
   }, 60_000);

   afterAll(async () => {
      await ws.cleanup();
   });

   for (const c of QUERY_SURFACE_CASES) {
      test(`${c.file}::${c.source} is ${c.queryable ? "" : "not "}queryable: ${c.why}`, () => {
         const result = results.find((r) => r.modelFilePath === c.file);
         assert.ok(result, `no compile result for ${c.file}`);
         assert.strictEqual(
            result.compileStatus,
            "SUCCESS",
            `compile failed: ${result.errorMessage}`,
         );
         // The source has to be in the file at all, or a missing marker would
         // pass for the wrong reason.
         const sources = (result.sourceInfos ?? []).map((info) => info.name);
         assert.ok(
            sources.includes(c.source),
            `${c.file} has no source ${c.source}: ${sources.join(", ")}`,
         );
         const marker = (result.sourceMetadataEntries ?? []).find(
            (m) =>
               m.metadataType === "NOT_QUERYABLE" && m.sourceName === c.source,
         );
         assert.strictEqual(marker === undefined, c.queryable);
         const servedFrom = marker?.servedFrom
            ? `${marker.servedFrom.modelFilePath}::${marker.servedFrom.sourceName}`
            : undefined;
         assert.strictEqual(servedFrom, c.servedFrom);
      });
   }
});

describe("a throw while reading one file's surface", () => {
   let ws: Workspace;
   let results: ModelCompileResult[];

   // The report-tag reader throws on notebooks/review.malloy's artifact note
   // (the only one naming `reviewed_orders`), standing in for any throw in the
   // tag parser.
   beforeAll(async () => {
      const real = report.readReportTag;
      const spy = spyOn(report, "readReportTag").mockImplementation(
         (notes: readonly string[]) => {
            if (notes.some((text) => text.includes("reviewed_orders ->"))) {
               throw new Error("tag parser exploded");
            }
            return real(notes);
         },
      );
      try {
         ({ ws, results } = await compileFixturePackage());
      } finally {
         spy.mockRestore();
      }
   }, 60_000);

   afterAll(async () => {
      await ws.cleanup();
   });

   test("ERRORs that file with the error, and only that file", () => {
      const review = results.find(
         (r) => r.modelFilePath === "notebooks/review.malloy",
      );
      assert.strictEqual(review?.compileStatus, "ERROR");
      assert.strictEqual(
         review?.errorMessage,
         "Error reading the query surface of notebooks/review.malloy: tag parser exploded",
      );
      const others = results.filter(
         (r) => r.modelFilePath !== "notebooks/review.malloy",
      );
      assert.ok(others.length > 0);
      assert.deepStrictEqual(
         others
            .filter((r) => r.compileStatus !== "SUCCESS")
            .map((r) => r.modelFilePath),
         [],
      );
   });
});

// derivedFromServed maps each report source to the one served source its
// chain reaches. That holds because a name in a compiled file has one declared
// base: Malloy refuses a second declaration, so no report source can derive
// from two served sources.
test("a report that declares one name over two served sources does not compile", async () => {
   const files: Record<string, string> = {
      "orders.malloy": `
source: orders is duckdb.sql("select 1 as id") extend {}
source: items is duckdb.sql("select 2 as id") extend {}
`,
      "index.malloy": `
import "orders.malloy"
export { orders, items }
`,
      "reports/both.malloy": `
## artifact { layout=linear tiles=["both -> { aggregate: n is count() }"] }
import "../orders.malloy"
source: both is orders extend {}
source: both is items extend {}
`,
   };
   const ws = await writeWorkspace(
      Object.entries(files).map(([relpath, contents]) => ({
         relpath,
         contents,
      })),
      "entities-query-surface-redefine-",
   );
   try {
      const packageFiles = Object.keys(files).map((relativePath) => ({
         packageRootPath: ws.dir,
         absolutePath: path.join(ws.dir, relativePath),
         relativePath,
      }));
      const lookup = await ConnectionLookup.getInstance([], ws.dir);
      const results = await packageCompilationService.compilePackageModelFiles(
         {
            modelFiles: packageFiles.filter((f) =>
               f.relativePath.endsWith(".malloy"),
            ),
            manifest:
               packageManagementService.readPackageManifest(packageFiles),
         },
         lookup,
      );
      const report = results.find(
         (r) => r.modelFilePath === "reports/both.malloy",
      );
      assert.strictEqual(report?.compileStatus, "ERROR");
      assert.match(report?.errorMessage ?? "", /Cannot redefine 'both'/);
   } finally {
      await ws.cleanup();
   }
}, 60_000);
