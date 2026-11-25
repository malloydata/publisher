// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `Model.admitsSourceByName`, the query route's queryability rule, over one
 * real package that exercises every way a file is or is not a served report
 * (`entities/__fixtures__/report_surface_package.ts`). For each (file, source)
 * it must say what the route does, and agree with the pre-compile boundary
 * check a query naming that source meets.
 */

import { afterAll, beforeAll, describe, expect, spyOn, test } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { REPORT_SURFACE_FILES } from "./entities/__fixtures__/report_surface_package";
import { NotQueryableError } from "../errors";
import { Package } from "./package";

/** [file, source, whether the query route runs a query naming it there, why]. */
const CASES: [string, string, boolean, string][] = [
   ["index.malloy", "orders", true, "the surface file exports it"],
   ["index.malloy", "orders_staging", false, "imported but not exported"],
   [
      "orders.malloy",
      "orders",
      false,
      "orders.malloy is not an entry point (not the surface, not a report)",
   ],
   ["orders.malloy", "orders_staging", false, "not an entry point"],
   [
      "dashboards/overview.malloy",
      "orders",
      true,
      "a report re-imports the declaration index.malloy exports",
   ],
   [
      "dashboards/overview.malloy",
      "open_orders",
      true,
      "the report builds it on an exported source",
   ],
   [
      "dashboards/overview.malloy",
      "orders_staging",
      false,
      "no surface file exports it",
   ],
   [
      "dashboards/overview.malloy",
      "staged",
      false,
      "built on a source no surface file exports",
   ],
   [
      "dashboards/overview.malloy",
      "scratch",
      false,
      "built straight off the connection, not on an exported source",
   ],
   [
      "dashboards/shadow.malloy",
      "orders",
      false,
      "same name as the exported source, but a different declaration",
   ],
   [
      "notebooks/review.malloy",
      "orders",
      true,
      "a linear report re-imports the exported declaration",
   ],
   [
      "notebooks/review.malloy",
      "reviewed_orders",
      true,
      "the linear report builds it on an exported source",
   ],
   [
      "notebooks/review.malloy",
      "orders_staging",
      false,
      "no surface file exports it",
   ],
   [
      "dashboards/notes.malloy",
      "noted_orders",
      false,
      "a broken note that mentions artifact does not make a report",
   ],
   [
      "dashboards/malformed.malloy",
      "malformed_orders",
      false,
      "an artifact tag that does not parse does not make a report",
   ],
   [
      "dashboards/detail.malloy",
      "detail_orders",
      false,
      "an untagged file is no report, even when it imports one",
   ],
   [
      "dashboards/query_tag.malloy",
      "query_tag_orders",
      false,
      "an artifact tag on a named query is not read, so no report",
   ],
   [
      "dashboards/chain.malloy",
      "chain_64",
      true,
      "64 derivation hops from an exported source",
   ],
   ["dashboards/chain.malloy", "chain_65", false, "65 hops, one past the walk"],
   [
      "dashboards/bare.malloy",
      "bare_orders",
      false,
      "a tag that lists no tiles makes no report",
   ],
   [
      "notebooks/legacy.malloy",
      "legacy_orders",
      false,
      "a tag that sets kind= makes no report",
   ],
   [
      "dashboards/shared.malloy",
      "orders",
      false,
      "an untagged file is not a report, so not an entry point",
   ],
   ["dashboards/shared.malloy", "shared_orders", false, "not an entry point"],
   [
      "reports/overview.malloy",
      "dup_orders",
      true,
      "a report sharing a basename with another is still served",
   ],
   [
      "reports/overview.malloy",
      "orders",
      true,
      "a grid report re-imports the exported declaration",
   ],
   [
      "reports/tileless.malloy",
      "tileless_orders",
      false,
      "a grid tag that names no tiles is not served",
   ],
   [
      "reports/empty.malloy",
      "empty_orders",
      false,
      "tiles=[] is a grid with no tiles, which is not served",
   ],
   [
      "reports/linear.malloy",
      "linear_orders",
      true,
      "layout=linear is a served report",
   ],
   [
      "top.malloy",
      "top_orders",
      true,
      "a report at the package root is served like any other",
   ],
   [
      "reports/named.malloy",
      "orders",
      true,
      "a report's named import from the surface file is the declaration it exports",
   ],
   [
      "reports/named.malloy",
      "named_open",
      true,
      "the report builds it on the source it imports by name",
   ],
   [
      "reports/aliased.malloy",
      "o",
      false,
      "an aliased import is admitted by no name",
   ],
   [
      "reports/aliased.malloy",
      "aliased_orders",
      false,
      "built on an alias, which is not admitted",
   ],
];

async function loadPackage(
   publisherJson: Record<string, unknown>,
): Promise<{ pkg: Package; dir: string; close: () => Promise<void> }> {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "admits-source-"));
   for (const [relpath, contents] of Object.entries(REPORT_SURFACE_FILES)) {
      fs.mkdirSync(path.dirname(path.join(dir, relpath)), { recursive: true });
      fs.writeFileSync(path.join(dir, relpath), contents);
   }
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: "admits-source", ...publisherJson }),
   );
   const { DuckDBConnection } = await import("@malloydata/db-duckdb");
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   const pkg = await Package.create(
      "env",
      "admits-source",
      dir,
      new Map([["duckdb", duckdb]]),
   );
   return {
      pkg,
      dir,
      close: async () => {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      },
   };
}

describe("under the root index.malloy surface", () => {
   let loaded: Awaited<ReturnType<typeof loadPackage>>;

   beforeAll(async () => {
      loaded = await loadPackage({});
   }, 60_000);

   afterAll(async () => {
      await loaded.close();
   });

   test.each(CASES)("%s::%s runs: %p (%s)", (file, source, runs) => {
      const model = loaded.pkg.getModel(file);
      expect(model).toBeDefined();
      expect(model!.admitsSourceByName(source)).toBe(runs);
   });

   test.each(CASES)(
      "%s::%s: the boundary a query naming it meets agrees",
      (file, source, runs) => {
         const model = loaded.pkg.getModel(file)!;
         const early = () => model.assertQueryBoundaryEarly(source);
         if (runs) expect(early).not.toThrow();
         else expect(early).toThrow(NotQueryableError);
      },
   );

   test("nothing under public/ is a report, so it is no entry point", () => {
      const route = loaded.pkg.entitiesQueryRoute();
      expect(route.boundaryActive).toBe(true);
      expect(route.isServedReport("public/page.malloy")).toBe(false);
      expect(
         loaded.pkg
            .getModel("public/page.malloy")
            ?.admitsSourceByName("page_orders") ?? false,
      ).toBe(false);
   });
});

describe.each([
   ["an empty explores", { explores: [] }],
   [
      'queryableSources "all"',
      { explores: ["index.malloy"], queryableSources: "all" },
   ],
])("with %s, nothing is curated", (_label, publisherJson) => {
   let loaded: Awaited<ReturnType<typeof loadPackage>>;

   beforeAll(async () => {
      loaded = await loadPackage(publisherJson);
   }, 60_000);

   afterAll(async () => {
      await loaded.close();
   });

   test("every source of every compiled file is admitted", () => {
      expect(loaded.pkg.entitiesQueryRoute().boundaryActive).toBe(false);
      for (const [file, source] of CASES) {
         expect(loaded.pkg.getModel(file)!.admitsSourceByName(source)).toBe(
            true,
         );
      }
   });
});

describe("the route's curation is computed once per boundary, not per source", () => {
   let loaded: Awaited<ReturnType<typeof loadPackage>>;

   beforeAll(async () => {
      loaded = await loadPackage({});
   }, 60_000);

   afterAll(async () => {
      await loaded.close();
   });

   test("asking about every source lists this file's sources at most once", () => {
      // The entities ask once per source; rebuilding the curated-name set per
      // call made a listing quadratic in the source count.
      const model = loaded.pkg.getModel("index.malloy")!;
      const getSources = spyOn(model, "getSources");
      const names = [
         ...(model.getSources(true) ?? []).map((s) => s.name!),
         ...Array.from({ length: 200 }, (_, i) => `absent_${i}`),
      ];
      getSources.mockClear();
      for (const name of names) model.admitsSourceByName(name);
      for (const name of names) model.curatedBasesOf(name);
      expect(getSources.mock.calls.length).toBeLessThanOrEqual(1);
      getSources.mockRestore();
   });

   test("a derivation chain is walked once per name, however often it is asked", () => {
      const model = loaded.pkg.getModel("dashboards/chain.malloy")!;
      const walk = spyOn(
         model as unknown as { walkCuratedBases: () => unknown },
         "walkCuratedBases",
      );
      const first = model.curatedBasesOf("chain_64");
      for (let i = 0; i < 50; i++) {
         expect(model.curatedBasesOf("chain_64")).toBe(first);
         expect(model.admitsSourceByName("chain_64")).toBe(true);
         expect(model.curatedBasesOf("chain_65")).toBeUndefined();
      }
      expect(walk.mock.calls.length).toBeLessThanOrEqual(2);
      walk.mockRestore();
   });

   test("a query's own edges are never answered from the memo", () => {
      const model = loaded.pkg.getModel("index.malloy")!;
      expect(model.curatedBasesOf("mine")).toBeUndefined();
      expect([
         ...(model.curatedBasesOf("mine", "source: mine is orders extend {}") ??
            []),
      ]).toEqual(["orders"]);
      expect(model.curatedBasesOf("mine")).toBeUndefined();
   });
});
