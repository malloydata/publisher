// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @fileoverview Integration test for re-rooting `#(index)` dimensions of
 * NON-QUERYABLE sources under a declared (queryable) source, compiling a real
 * multi-file Malloy package against in-process DuckDB.
 *
 * With `queryableSources: "declared"`, only sources in the manifest's
 * `explores` AND a declared file's `export {}` closure are queryable. A
 * dimension rooted at a non-queryable source can't be sampled directly
 * (`run: <source> -> …` 404s), so the entities must carry a copy of that
 * dimension rooted at every DECLARED source that joins it — otherwise an
 * indexer that skips non-queryable sources drops it and the dimension is
 * silently lost.
 *
 * The recursion re-roots a *transitively*-imported join target (one not
 * reachable as a top-level explore). This also pins a declared source that
 * DIRECTLY joins a *top-level* imported (non-queryable) source, plus a
 * multi-level deep import chain — a bridge source exposing many imported
 * sources through a single declared entry point.
 *
 * Topology compiled here (declared entry = `analytics.malloy`):
 *
 *   analytics  ──join──▶  order_items  ──join──▶  inventory_items
 *   (declared,            (top-level import,       (transitive import,
 *    queryable)            NON-queryable)           NON-queryable)
 *
 * Only `analytics` is queryable. `order_items.oi_label` (#index) and
 * `inventory_items.product_category` (#index) must each surface re-rooted under
 * `analytics`.
 *
 * Files use ordinary `.malloy` names throughout: the author declares the entry
 * point in `publisher.json` and manages imports/exports across normal files —
 * there is no need (and we don't aim to support) the older trick of hiding a
 * model behind a non-`.malloy` filename.
 *
 * No external services — DuckDB runs in-process over CSVs written to a tmpdir.
 */

import { afterEach, beforeEach, describe, test } from "bun:test";
import assert from "node:assert";
import path from "path";
import { writeWorkspace, type Workspace } from "./test_support";
import { packageCompilationService } from "./test_support";
import { ConnectionLookup } from "./test_support";
import { packageManagementService } from "./test_support";
import type { PackageManifest } from "./query_surface";

const LEAF = `
source: inventory_items is duckdb.table('inventory.csv') extend {
  dimension:
    #(index)
    product_category is upper(cat)
}
`;

const ORDER_HUB = `
import "inventory_items.malloy"
source: order_items is duckdb.table('order_items.csv') extend {
  join_one: inventory_items on inv_id = inventory_items.id
  dimension:
    #(index)
    oi_label is concat('oi-', inv_id::string)
}
`;

// The declared entry. `analytics` is locally defined (so auto-exported →
// queryable) and DIRECTLY joins the top-level, non-queryable `order_items`.
const ANALYTICS = `
import "order_hub.malloy"
source: analytics is duckdb.table('analytics.csv') extend {
  join_one: order_items on oi_id = order_items.id
}
`;

describe("re-rooting non-queryable #(index) dims under a declared source", () => {
   let ws: Workspace;
   let pkgRoot: string;

   beforeEach(async () => {
      ws = await writeWorkspace(
         [
            { relpath: "inventory.csv", contents: "id,cat\n1,shoes\n2,hats\n" },
            {
               relpath: "order_items.csv",
               contents: "id,inv_id,oi_id\n10,1,100\n11,2,101\n",
            },
            {
               relpath: "analytics.csv",
               contents: "id,oi_id\n1000,100\n1001,101\n",
            },
            { relpath: "inventory_items.malloy", contents: LEAF },
            { relpath: "order_hub.malloy", contents: ORDER_HUB },
            { relpath: "analytics.malloy", contents: ANALYTICS },
         ],
         "entities-queryable-reroot-",
      );
      pkgRoot = ws.dir;
   });

   afterEach(async () => {
      await ws.cleanup();
   });

   // Compile analytics.malloy with `analytics.malloy` as the only declared
   // explore, so order_items + inventory_items are non-queryable.
   async function compileEntities() {
      const lookup = await ConnectionLookup.getInstance([], pkgRoot);
      const manifest: PackageManifest = {
         explores: ["analytics.malloy"],
         queryableSources: "declared",
      };
      const [result] = await packageCompilationService.compilePackageModelFiles(
         {
            modelFiles: [
               {
                  packageRootPath: pkgRoot,
                  absolutePath: path.join(pkgRoot, "analytics.malloy"),
                  relativePath: "analytics.malloy",
               },
            ],
            manifest,
         },
         lookup,
      );
      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `compile failed: ${result.errorMessage}`,
      );
      return result.entities ?? [];
   }

   const describeEntities = (
      entities: { source?: string | null; name: string }[],
   ) => entities.map((e) => `${e.source}::${e.name}`).join(", ");

   test("re-roots a directly-joined top-level non-queryable source's #(index) dim under the declared source", async () => {
      // `order_items` is a top-level import (so the
      // old `!targetIsAtTopLevel` recursion did NOT fire) but is non-queryable,
      // and `analytics` joins it directly. Its #(index) dim must surface rooted
      // at the queryable `analytics`, as `order_items.oi_label`.
      const entities = await compileEntities();

      const reRooted = entities.find(
         (e) => e.source === "analytics" && e.name === "order_items.oi_label",
      );
      assert.ok(
         reRooted,
         `expected re-rooted analytics::order_items.oi_label; got: ${describeEntities(entities)}`,
      );
      assert.strictEqual(reRooted.field_type, "dimension");
      assert.ok(
         reRooted.tags?.some((t) => t.includes("#(index")),
         `re-rooted dim lost its #(index) tag: ${reRooted.tags}`,
      );
   }, 60_000);

   test("re-roots a DEEP-chain dim two imports deep under the declared source", async () => {
      // inventory_items is two joins deep (analytics → order_items →
      // inventory_items) across a multi-level import chain. Its #(index) dim
      // must surface rooted at `analytics` with the full join path.
      const entities = await compileEntities();

      const deep = entities.find(
         (e) =>
            e.source === "analytics" &&
            e.name === "order_items.inventory_items.product_category",
      );
      assert.ok(
         deep,
         `expected re-rooted analytics::order_items.inventory_items.product_category; got: ${describeEntities(entities)}`,
      );
      assert.strictEqual(deep.field_type, "dimension");
      assert.ok(
         deep.tags?.some((t) => t.includes("#(index")),
         `deep re-rooted dim lost its #(index) tag: ${deep.tags}`,
      );
   }, 60_000);

   test("re-rooted #(index) dims report a stable fingerprint across compiles", async () => {
      // The entity `id` is a per-run `randomUUID()`; a cross-version-stable id
      // is derived by the host from the `dim_index_fingerprint` (compiled-SQL
      // hash + required-filter set) stamped on each `#(index)` dimension. So
      // index reuse across versions hinges on that
      // fingerprint being (a) PRESENT — an absent one means the fingerprint
      // fragment failed to compile and the dim would fall back to a fresh id
      // every publish, orphaning the prior version's value rows — and (b) STABLE
      // across compiles. A re-rooted copy (join-qualified path under `analytics`)
      // is the case that most easily regresses, since its fragment must compile
      // the join-qualified field path.
      const first = await compileEntities();
      const second = await compileEntities();
      const pick = (
         entities: {
            source?: string | null;
            name: string;
            dim_index_fingerprint?: { sql_fingerprint?: string } | null;
         }[],
         name: string,
      ) => entities.find((e) => e.source === "analytics" && e.name === name);

      for (const name of [
         "order_items.oi_label",
         "order_items.inventory_items.product_category",
      ]) {
         const a = pick(first, name);
         const b = pick(second, name);
         assert.ok(a && b, `missing re-rooted analytics::${name}`);
         assert.ok(
            a.dim_index_fingerprint?.sql_fingerprint,
            `expected a compiled-SQL fingerprint on analytics::${name}; ` +
               `got none (fingerprint fragment failed to compile → the dim would ` +
               `fall back to a fresh random id every publish)`,
         );
         assert.strictEqual(
            a.dim_index_fingerprint?.sql_fingerprint,
            b.dim_index_fingerprint?.sql_fingerprint,
            `fingerprint for analytics::${name} is not stable across compiles`,
         );
      }
   }, 120_000);

   test("every #(index) dimension is reachable from a queryable (declared) source", async () => {
      // Contract check: no #(index) dimension is left rooted ONLY at a
      // non-queryable source. Each indexed dimension must have at least one copy
      // whose `source` is the declared `analytics` (the sole queryable source),
      // so an indexer that skips non-queryable sources keeps it and can query it.
      const entities = await compileEntities();

      const indexedDims = entities.filter(
         (e) =>
            e.field_type === "dimension" &&
            e.tags?.some((t) => t.includes("#(index")),
      );
      // The two declared #(index) dims, each potentially appearing both rooted
      // at its non-queryable source and re-rooted under analytics.
      const logicalDims = new Set(
         indexedDims.map((e) =>
            e.name.includes(".") ? e.name.split(".").pop() : e.name,
         ),
      );
      assert.deepStrictEqual(
         [...logicalDims].sort(),
         ["oi_label", "product_category"],
         `unexpected indexed dims: ${describeEntities(indexedDims)}`,
      );

      for (const dim of [...logicalDims]) {
         const reachable = indexedDims.some(
            (e) => e.source === "analytics" && e.name.endsWith(dim as string),
         );
         assert.ok(
            reachable,
            `#(index) dim '${dim}' has no copy rooted at a queryable source: ${describeEntities(indexedDims)}`,
         );
      }
   }, 60_000);
});

// With no `explores` in publisher.json, a root `index.malloy`
// is the surface. Every query enters through an import there, so this pins
// that the re-export reaches `exportedExplores` on the index file and nothing
// in the imported file stays queryable.
const ORDERS = `
source: orders is duckdb.table('orders.csv') extend {
  dimension:
    #(index)
    status_label is upper(status)
}
source: orders_staging is duckdb.table('orders.csv')
`;

const INDEX = `
import "orders.malloy"
export { orders }
`;

describe("a root index.malloy as the package surface", () => {
   let ws: Workspace;

   beforeEach(async () => {
      ws = await writeWorkspace(
         [
            {
               relpath: "orders.csv",
               contents: "id,status\n1,open\n2,closed\n",
            },
            { relpath: "orders.malloy", contents: ORDERS },
            { relpath: "index.malloy", contents: INDEX },
            { relpath: "publisher.json", contents: '{"name": "sales"}' },
         ],
         "entities-index-surface-",
      );
   });

   afterEach(async () => {
      await ws.cleanup();
   });

   const pointer = (relativePath: string) => ({
      packageRootPath: ws.dir,
      absolutePath: path.join(ws.dir, relativePath),
      relativePath,
   });

   async function compile(relativePath: string) {
      const packageFiles = [
         "orders.csv",
         "orders.malloy",
         "index.malloy",
         "publisher.json",
      ].map(pointer);
      const manifest =
         packageManagementService.readPackageManifest(packageFiles);
      assert.deepStrictEqual(manifest?.explores, ["index.malloy"]);
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
      const result = results.find((r) => r.modelFilePath === relativePath);
      assert.ok(result, `no result for ${relativePath}`);
      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `compile failed: ${result.errorMessage}`,
      );
      return result;
   }

   const notQueryable = (result: Awaited<ReturnType<typeof compile>>) =>
      (result.sourceMetadataEntries ?? [])
         .filter((m) => m.metadataType === "NOT_QUERYABLE")
         .map((m) => m.sourceName)
         .sort();

   test("only the exported source is queryable through index.malloy, and its #(index) dim stays rooted there", async () => {
      const result = await compile("index.malloy");
      // index.malloy's compiled model also carries the imported
      // `orders_staging`; it is outside the export, so Publisher 404s it.
      assert.deepStrictEqual(notQueryable(result), ["orders_staging"]);
      const dim = (result.entities ?? []).find(
         (e) => e.source === "orders" && e.name === "status_label",
      );
      assert.ok(dim, "expected orders::status_label from index.malloy");
      assert.ok(dim.tags?.some((t) => t.includes("#(index")));
   }, 60_000);

   test("every source in the imported file is off the surface", async () => {
      const result = await compile("orders.malloy");
      assert.deepStrictEqual(notQueryable(result), [
         "orders",
         "orders_staging",
      ]);
   }, 60_000);
});

// A dashboard reads the surface and adds nothing to it, even
// when `explores` lists it, which is the shape 0.7.0 advised for serving
// dashboards beside an index.malloy. Only a tagged file is a dashboard: an
// untagged file under `dashboards/` that `explores` lists is an ordinary
// surface file.
//
// Every file here imports `orders` from orders.malloy, the same declaration
// index.malloy exports, so `orders` is queryable in all of them. What tells a
// dashboard apart is its own export: refused in a dashboard, queryable in an
// ordinary listed file.
const TAGGED_DASHBOARD = `
## artifact { tiles=["staging -> by_status"] }
import "../orders.malloy"
source: staging is orders_staging extend {
  view: by_status is { group_by: status }
}
export { staging }
`;

// `artifact` is not the first word, which a text check anchored on it missed.
const TITLED_DASHBOARD = `
## title="Q3" artifact { tiles=["titled -> by_status"] }
import "../orders.malloy"
source: titled is orders_staging extend {
  view: by_status is { group_by: status }
}
export { titled }
`;

// The old single-query form: the tag sits on a query, after another property.
// Since Publisher 0.9.8 the tag is read at model level only, so this is an
// ordinary listed file.
const QUERY_TAGGED_DASHBOARD = `
import "../orders.malloy"
source: single is orders_staging
# dashboard artifact
query: overview is single -> { group_by: status }
export { single }
`;

// A tag that does not parse. Publisher's parser finds no artifact tag in it,
// so this is an ordinary listed file and its export is queryable.
const MALFORMED_TAG_DASHBOARD = `
## title="Q3 artifact { tiles=[
import "../orders.malloy"
source: malformed is orders_staging
export { malformed }
`;

// A broken tag on the query, and a tag that parses and mentions artifact in a
// value. Neither is an artifact tag, so the export stays queryable.
const UNRELATED_BROKEN_TAG_FILE = `
## note="exported to the artifact store"
import "../orders.malloy"
source: unrelated is orders_staging
# title="Q3
query: overview is unrelated -> { group_by: status }
export { unrelated }
`;

// Two model tags: one parses and mentions artifact in a value, the other is
// broken. Publisher keeps the line that parses on its own and finds no
// artifact tag in it, so the export stays queryable.
const SIBLING_BROKEN_TAG_FILE = `
## note="exported to the artifact store"
## title="Q3
import "../orders.malloy"
source: sibling is orders_staging
export { sibling }
`;

const COMMENTED_TAG_FILE = `
// ## artifact { tiles=[] }
import "../orders.malloy"
source: commented is orders_staging
export { commented }
`;

const UNTAGGED_DASHBOARDS_FILE = `
import "../orders.malloy"
source: listed is orders_staging
export { listed }
`;

describe("a dashboard that explores lists", () => {
   let ws: Workspace;
   const files = [
      "orders.csv",
      "orders.malloy",
      "index.malloy",
      "dashboards/overview.malloy",
      "dashboards/old.malloy",
      "dashboards/titled.malloy",
      "dashboards/single.malloy",
      "dashboards/malformed.malloy",
      "dashboards/commented.malloy",
      "dashboards/unrelated.malloy",
      "dashboards/sibling.malloy",
      "publisher.json",
   ];

   beforeEach(async () => {
      ws = await writeWorkspace(
         [
            {
               relpath: "orders.csv",
               contents: "id,status\n1,open\n2,closed\n",
            },
            { relpath: "orders.malloy", contents: ORDERS },
            { relpath: "index.malloy", contents: INDEX },
            {
               relpath: "dashboards/overview.malloy",
               contents: TAGGED_DASHBOARD,
            },
            {
               relpath: "dashboards/old.malloy",
               contents: UNTAGGED_DASHBOARDS_FILE,
            },
            {
               relpath: "dashboards/titled.malloy",
               contents: TITLED_DASHBOARD,
            },
            {
               relpath: "dashboards/single.malloy",
               contents: QUERY_TAGGED_DASHBOARD,
            },
            {
               relpath: "dashboards/malformed.malloy",
               contents: MALFORMED_TAG_DASHBOARD,
            },
            {
               relpath: "dashboards/commented.malloy",
               contents: COMMENTED_TAG_FILE,
            },
            {
               relpath: "dashboards/unrelated.malloy",
               contents: UNRELATED_BROKEN_TAG_FILE,
            },
            {
               relpath: "dashboards/sibling.malloy",
               contents: SIBLING_BROKEN_TAG_FILE,
            },
            {
               relpath: "publisher.json",
               contents: JSON.stringify({
                  name: "sales",
                  explores: [
                     "index.malloy",
                     "dashboards/overview.malloy",
                     "dashboards/old.malloy",
                     "dashboards/titled.malloy",
                     "dashboards/single.malloy",
                     "dashboards/malformed.malloy",
                     "dashboards/commented.malloy",
                     "dashboards/unrelated.malloy",
                     "dashboards/sibling.malloy",
                  ],
               }),
            },
         ],
         "entities-dashboard-surface-",
      );
   });

   afterEach(async () => {
      await ws.cleanup();
   });

   const pointer = (relativePath: string) => ({
      packageRootPath: ws.dir,
      absolutePath: path.join(ws.dir, relativePath),
      relativePath,
   });

   async function notQueryable(relativePath: string) {
      const manifest = packageManagementService.readPackageManifest(
         files.map(pointer),
      );
      const lookup = await ConnectionLookup.getInstance([], ws.dir);
      const results = await packageCompilationService.compilePackageModelFiles(
         {
            modelFiles: files.filter((f) => f.endsWith(".malloy")).map(pointer),
            manifest,
         },
         lookup,
      );
      const result = results.find((r) => r.modelFilePath === relativePath);
      assert.ok(result, `no result for ${relativePath}`);
      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `compile failed: ${result.errorMessage}`,
      );
      return (result.sourceMetadataEntries ?? [])
         .filter((m) => m.metadataType === "NOT_QUERYABLE")
         .map((m) => m.sourceName)
         .sort();
   }

   // `orders` is the declaration index.malloy exports: Publisher runs it in a
   // report too, but the index advertises it once, from index.malloy.
   test("a tagged report's own export is not queryable", async () => {
      assert.deepStrictEqual(await notQueryable("dashboards/overview.malloy"), [
         "orders",
         "orders_staging",
         "staging",
      ]);
   }, 60_000);

   test("a model tag with artifact after another property is a report", async () => {
      assert.deepStrictEqual(await notQueryable("dashboards/titled.malloy"), [
         "orders",
         "orders_staging",
         "titled",
      ]);
   }, 60_000);

   test("an artifact tag on a query is not read, so the listed file keeps its export queryable", async () => {
      assert.deepStrictEqual(await notQueryable("dashboards/single.malloy"), [
         "orders_staging",
      ]);
   }, 60_000);

   test("a tag that does not parse is not a dashboard", async () => {
      assert.deepStrictEqual(
         await notQueryable("dashboards/malformed.malloy"),
         ["orders_staging"],
      );
   }, 60_000);

   test("a broken tag does not make an unrelated mention of artifact a dashboard", async () => {
      assert.deepStrictEqual(
         await notQueryable("dashboards/unrelated.malloy"),
         ["orders_staging"],
      );
   }, 60_000);

   test("a broken model tag does not make a sibling tag's mention of artifact a dashboard", async () => {
      assert.deepStrictEqual(await notQueryable("dashboards/sibling.malloy"), [
         "orders_staging",
      ]);
   }, 60_000);

   test("a commented-out tag is not a dashboard", async () => {
      assert.deepStrictEqual(
         await notQueryable("dashboards/commented.malloy"),
         ["orders_staging"],
      );
   }, 60_000);

   test("an untagged file under dashboards/ keeps its export queryable", async () => {
      assert.deepStrictEqual(await notQueryable("dashboards/old.malloy"), [
         "orders_staging",
      ]);
   }, 60_000);
});
