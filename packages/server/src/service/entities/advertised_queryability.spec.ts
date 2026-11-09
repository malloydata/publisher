// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `advertisedQueryability`: the entities report a source queryable exactly
 * when the query route runs it, except that under a query boundary nothing a
 * served report reads is advertised; each such source names the served source
 * it stands for instead.
 */

import { describe, expect, test } from "bun:test";
import { REPORT_SURFACE_FILES } from "./__fixtures__/report_surface_package";
import { advertisedQueryability, type EntitiesQueryRoute } from "./extract";
import type { ModelCompileResult } from "./internal_types";
import { compileFixture } from "./test_support";

const ORDERS = { modelFilePath: "index.malloy", sourceName: "orders" };

/** A route over one report and one ordinary file, each admitting `admitted`. */
const route = (
   boundaryActive: boolean,
   admitted: string[],
): EntitiesQueryRoute => ({
   boundaryActive,
   admitsSource: (_modelPath, name) => admitted.includes(name),
   isServedReport: (modelPath) => modelPath === "report.malloy",
   servedSourceOf: (_modelPath, name) =>
      name === "orders" || name === "open_orders" ? ORDERS : undefined,
});

describe("advertisedQueryability", () => {
   const names = ["orders", "open_orders", "scratch"];

   test("an ordinary file advertises exactly what the route runs", () => {
      const out = advertisedQueryability(
         route(true, ["orders"]),
         "index.malloy",
         names,
      );
      expect([...out.notQueryable]).toEqual(["open_orders", "scratch"]);
      expect([...out.servedFrom]).toEqual([]);
   });

   test("a served report advertises nothing, though the route runs some of it, and names each one's served source", () => {
      const out = advertisedQueryability(
         route(true, ["orders", "open_orders"]),
         "report.malloy",
         names,
      );
      expect([...out.notQueryable]).toEqual(names);
      expect([...out.servedFrom]).toEqual([
         ["orders", ORDERS],
         ["open_orders", ORDERS],
      ]);
   });

   test("without a boundary a report is read as no report", () => {
      const out = advertisedQueryability(
         route(false, names),
         "report.malloy",
         names,
      );
      expect([...out.notQueryable]).toEqual([]);
      expect([...out.servedFrom]).toEqual([]);
   });
});

/** The NOT_QUERYABLE marker of `file::source`, or undefined when it is advertised. */
const marker = (results: ModelCompileResult[], file: string, source: string) =>
   results
      .find((r) => r.modelFilePath === file)
      ?.sourceMetadataEntries.find(
         (m) => m.sourceName === source && m.metadataType === "NOT_QUERYABLE",
      );

test("in a real package, a report source the route runs is reported not queryable, with its served source", async () => {
   const results = await compileFixture(REPORT_SURFACE_FILES, {
      explores: ["index.malloy"],
   });
   // Served from index.malloy: advertised there, once.
   expect(marker(results, "index.malloy", "orders")).toBeUndefined();
   // The route runs each of these (model_admits_source.spec.ts); the entities
   // stand index.malloy's `orders` in for them.
   for (const [file, source] of [
      ["dashboards/overview.malloy", "orders"],
      ["dashboards/overview.malloy", "open_orders"],
      ["notebooks/review.malloy", "reviewed_orders"],
      ["dashboards/chain.malloy", "chain_64"],
      ["reports/named.malloy", "named_open"],
   ]) {
      expect(marker(results, file, source)?.servedFrom).toEqual(ORDERS);
   }
   // An aliased import is refused by the route, but it is still the served
   // declaration; what is built on it names nothing.
   expect(marker(results, "reports/aliased.malloy", "o")?.servedFrom).toEqual(
      ORDERS,
   );
   const aliased = marker(results, "reports/aliased.malloy", "aliased_orders");
   expect(aliased).toBeDefined();
   expect(aliased?.servedFrom).toBeUndefined();
   // One past the walk proves nothing, so it names nothing either.
   const tooFar = marker(results, "dashboards/chain.malloy", "chain_65");
   expect(tooFar).toBeDefined();
   expect(tooFar?.servedFrom).toBeUndefined();
}, 120_000);

// A report source names the ONE served source its chain reaches. That holds
// because a name in a compiled file has one declared base: Malloy refuses a
// second declaration, so no report source can derive from two served sources.
test("a report that declares one name over two served sources does not compile", async () => {
   const results = await compileFixture(
      {
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
      },
      { explores: ["index.malloy"] },
   );
   const report = results.find(
      (r) => r.modelFilePath === "reports/both.malloy",
   );
   expect(report?.compileStatus).toBe("ERROR");
   expect(report?.errorMessage ?? "").toMatch(/Cannot redefine 'both'/);
}, 120_000);
