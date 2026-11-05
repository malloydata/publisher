// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @fileoverview Focused tests for ``extractEntitiesFromExplores``'s
 * top-level walk + join-recursion behaviour, real-compiling Malloy
 * snippets via the production ``Malloy.compile`` path and an in-tempdir
 * DuckDB.
 *
 * Two things this file pins:
 *
 *   1. The fix: when a join's target source is NOT in
 *      ``compiled.explores`` for this entry, ``getEntitiesFromExplore``
 *      recurses into the join's nested struct so the joined-source
 *      fields land in the entities under a dotted source path
 *      (e.g. ``bridge_model.order_items.Created_at``). Without it a
 *      bridge model over a deep import chain reports no entities for
 *      joined-source fields even though queries resolve them.
 *
 *   2. Two upstream-Malloy contracts the fix relies on:
 *        a. Transitive imports do NOT propagate sources past the first
 *           hop (``modelDef.contents`` only contains the entry's own
 *           sources + its direct imports).
 *        b. ``structDef`` trees are finite — forward references error
 *           at compile, so structural cycles in joins are impossible.
 *           Recursion in the fix therefore terminates by construction
 *           and needs no depth cap.
 *
 * If upstream Malloy changes either contract, the corresponding test
 * here trips and the fix's design assumptions need re-evaluation.
 */

import { afterEach, beforeEach, describe, test } from "bun:test";
import assert from "node:assert";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Malloy } from "@malloydata/malloy";
import { FileURLReader } from "./malloy_indexing_helpers";
import { ConnectionLookup } from "./test_support";
import { packageSanitizationService } from "./package_sanitization";
import type { Entity } from "./internal_types";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type PackageLayout = {
   entryRelativePath: string;
   files: Record<string, string>;
   dataFiles?: Record<string, string>;
};

async function runExtraction(
   layout: PackageLayout,
   packageRoot: string,
): Promise<Entity[]> {
   for (const [rel, contents] of Object.entries(layout.files)) {
      const abs = path.join(packageRoot, rel);
      await mkdir(path.dirname(abs), { recursive: true });
      await writeFile(abs, contents, "utf-8");
   }
   for (const [rel, contents] of Object.entries(layout.dataFiles ?? {})) {
      const abs = path.join(packageRoot, rel);
      await mkdir(path.dirname(abs), { recursive: true });
      await writeFile(abs, contents, "utf-8");
   }

   const entryAbs = path.join(packageRoot, layout.entryRelativePath);
   const entryDir = path.dirname(entryAbs);
   const source = layout.files[layout.entryRelativePath];
   if (source === undefined) {
      throw new Error(
         `entryRelativePath '${layout.entryRelativePath}' not in layout.files`,
      );
   }

   const urlReader = new FileURLReader();
   const modelUrl = new URL("file://" + entryAbs);
   const importBaseURL = new URL("file://" + entryDir + "/");
   const connections = await ConnectionLookup.getInstance([], packageRoot);

   const compiled = await Malloy.compile({
      urlReader,
      connections,
      parse: await Malloy.parse({
         url: modelUrl,
         urlReader,
         options: { importBaseURL },
      }),
   });

   return packageSanitizationService.extractEntitiesFromExplores(
      compiled.explores,
      layout.entryRelativePath,
      packageRoot,
      source,
   );
}

/**
 * Compile a layout and return the raw ``modelDef`` contents/exports
 * plus the explore-name list. Used for the upstream-Malloy contract
 * tests that pin what ``compiled.explores`` actually surfaces.
 */
async function dumpCompileResult(
   layout: PackageLayout,
   packageRoot: string,
): Promise<{ contents: string[]; exports: string[]; exploreNames: string[] }> {
   for (const [rel, contents] of Object.entries(layout.files)) {
      const abs = path.join(packageRoot, rel);
      await mkdir(path.dirname(abs), { recursive: true });
      await writeFile(abs, contents, "utf-8");
   }
   for (const [rel, contents] of Object.entries(layout.dataFiles ?? {})) {
      const abs = path.join(packageRoot, rel);
      await mkdir(path.dirname(abs), { recursive: true });
      await writeFile(abs, contents, "utf-8");
   }
   const entryAbs = path.join(packageRoot, layout.entryRelativePath);
   const entryDir = path.dirname(entryAbs);
   const urlReader = new FileURLReader();
   const connections = await ConnectionLookup.getInstance([], packageRoot);
   const compiled = await Malloy.compile({
      urlReader,
      connections,
      parse: await Malloy.parse({
         url: new URL("file://" + entryAbs),
         urlReader,
         options: { importBaseURL: new URL("file://" + entryDir + "/") },
      }),
   });
   const md = (
      compiled as unknown as {
         _modelDef: { contents: Record<string, unknown>; exports: string[] };
      }
   )._modelDef;
   return {
      contents: Object.keys(md.contents).sort(),
      exports: [...md.exports].sort(),
      exploreNames: compiled.explores.map((e) => e.name).sort(),
   };
}

type Row = {
   source: string | null;
   name: string;
   field_type: string;
   join_target?: string | null;
   nested?: boolean;
};

function projectForSnapshot(entities: Entity[]): Row[] {
   return entities
      .map((e) => {
         const meta = e.join_source_metadata;
         const r: Row = {
            source: e.source ?? null,
            name: e.name,
            field_type: e.field_type,
         };
         if (meta) {
            r.join_target = meta.original_source_name ?? null;
            r.nested = meta.is_nested_index ?? false;
         }
         return r;
      })
      .sort((a, b) => {
         const s = (a.source ?? "").localeCompare(b.source ?? "");
         if (s !== 0) return s;
         const ft = a.field_type.localeCompare(b.field_type);
         if (ft !== 0) return ft;
         return a.name.localeCompare(b.name);
      });
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const PARENT_CSV =
   "id,order_id,Stage,region\n1,o1,users,us\n2,o2,order_items,us\n3,o3,events,eu\n";

const ORDER_ITEMS_CSV =
   "order_id,user_id,region,amount,Created_at\no1,u1,us,10,2026-01-01\no2,u2,us,20,2026-02-01\no3,u3,eu,5,2026-03-01\n";

const USERS_CSV = "user_id,Country,Age\nu1,US,30\nu2,US,40\nu3,DE,25\n";

function parentModelStanza(joinClause: string): string {
   return `
source: parent_model is duckdb.table('parents.csv') extend {
  primary_key: id
  ${joinClause}
}
`;
}

const ORDER_ITEMS_DECL = `
source: order_items is duckdb.table('order_items.csv') extend {
  primary_key: order_id
  measure: order_count is count()
  measure: total_amount is amount.sum()
}
`;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("package-sanitization: harness smoke", () => {
   let packageRoot: string;
   beforeEach(async () => {
      packageRoot = await mkdtemp(path.join(tmpdir(), "psmoke-"));
   });
   afterEach(async () => {
      await rm(packageRoot, { recursive: true, force: true });
   });

   test("trivial single-source package extracts the expected fields", async () => {
      const layout: PackageLayout = {
         entryRelativePath: "model.malloy",
         files: {
            "model.malloy":
               `source: orders is duckdb.table('data.csv') extend {\n` +
               `  measure: order_count is count()\n` +
               `}\n`,
         },
         dataFiles: { "data.csv": "region,amount\nus,10\nus,20\neu,5\n" },
      };
      const snap = projectForSnapshot(await runExtraction(layout, packageRoot));
      // projectForSnapshot sorts by (source, field_type, name) — columns
      // before measures within the same source.
      assert.deepStrictEqual(snap, [
         { source: "orders", name: "amount", field_type: "column" },
         { source: "orders", name: "region", field_type: "column" },
         { source: "orders", name: "order_count", field_type: "measure" },
      ]);
   });
});

describe("package-sanitization: upstream-Malloy contracts the fix depends on", () => {
   let packageRoot: string;
   beforeEach(async () => {
      packageRoot = await mkdtemp(path.join(tmpdir(), "pcontract-"));
   });
   afterEach(async () => {
      await rm(packageRoot, { recursive: true, force: true });
   });

   test("CONTRACT: transitive imports do NOT propagate (4-deep chain)", async () => {
      // entry → bridge → extended/order_items → sources/order_items_source.
      // We expect ``compiled.explores`` to contain ONLY the entry's own
      // source + its DIRECT import. Sources reachable via ≥2 hops never
      // reach the top-level namespace. This is the upstream behavior
      // that motivates the join-recursion fix; if Malloy ever starts
      // propagating transitive imports, this test trips and the fix's
      // recursion gate may become unnecessary.
      const layout: PackageLayout = {
         entryRelativePath: "published/explore/model.malloy",
         files: {
            "sources/users_source.malloy":
               `source: users_source is duckdb.table('users.csv') extend {\n` +
               `  primary_key: user_id\n` +
               `  measure: user_count is count()\n` +
               `}\n`,
            "sources/order_items_source.malloy":
               `import "./users_source.malloy"\n` +
               `source: order_items_source is duckdb.table('order_items.csv') extend {\n` +
               `  measure: order_count is count()\n` +
               `  measure: total_amount is amount.sum()\n` +
               `  join_one: users_source with user_id\n` +
               `}\n`,
            "extended/order_items.malloy":
               `import "../sources/order_items_source.malloy"\n` +
               `source: order_items is order_items_source extend { primary_key: order_id }\n`,
            "models/bridge.malloy":
               `import "../extended/order_items.malloy"\n` +
               parentModelStanza(
                  `join_one: order_items on Stage = "order_items"`,
               ),
            "published/explore/model.malloy":
               `import "../../models/bridge.malloy"\n` +
               `source: parent_model_explore is parent_model\n`,
         },
         dataFiles: {
            "parents.csv": PARENT_CSV,
            "order_items.csv": ORDER_ITEMS_CSV,
            "users.csv": USERS_CSV,
         },
      };

      const result = await dumpCompileResult(layout, packageRoot);
      assert.deepStrictEqual(result.contents, [
         "parent_model",
         "parent_model_explore",
      ]);
      assert.deepStrictEqual(result.exploreNames, [
         "parent_model",
         "parent_model_explore",
      ]);
      assert.deepStrictEqual(result.exports, ["parent_model_explore"]);
   });

   test("CONTRACT: direct (1-deep) imports DO surface at top level", async () => {
      // Contrast with the 4-deep test above: when ``order_items`` is a
      // direct import of the entry, it lands in ``modelDef.contents``
      // (marked exported:false) and shows up in ``compiled.explores``.
      // This is why the fix's "joined source already at top level" gate
      // works for the common case: those joined fields ARE walked,
      // just via the top-level pass instead of through join recursion.
      const layout: PackageLayout = {
         entryRelativePath: "model.malloy",
         files: {
            "order_items.malloy": ORDER_ITEMS_DECL,
            "model.malloy":
               `import "./order_items.malloy"\n` +
               parentModelStanza(`join_one: order_items with order_id`),
         },
         dataFiles: {
            "parents.csv": PARENT_CSV,
            "order_items.csv": ORDER_ITEMS_CSV,
         },
      };

      const result = await dumpCompileResult(layout, packageRoot);
      assert.deepStrictEqual(result.contents, ["order_items", "parent_model"]);
      assert.deepStrictEqual(result.exploreNames, [
         "order_items",
         "parent_model",
      ]);
      assert.deepStrictEqual(result.exports, ["parent_model"]);
   });
});

describe("package-sanitization: fix behaviour (transitive-import join recursion)", () => {
   let packageRoot: string;
   beforeEach(async () => {
      packageRoot = await mkdtemp(path.join(tmpdir(), "pfix-"));
   });
   afterEach(async () => {
      await rm(packageRoot, { recursive: true, force: true });
   });

   /** Bridge-model layout: 4-deep chain + inner join on the deeply-imported
    *  ``order_items_source`` (the inner join exercises second-level recursion). */
   function bridgeModelLayout(): PackageLayout {
      return {
         entryRelativePath: "published/explore/model.malloy",
         files: {
            "sources/users_source.malloy":
               `source: users_source is duckdb.table('users.csv') extend {\n` +
               `  primary_key: user_id\n` +
               `  measure: user_count is count()\n` +
               `}\n`,
            "sources/order_items_source.malloy":
               `import "./users_source.malloy"\n` +
               `source: order_items_source is duckdb.table('order_items.csv') extend {\n` +
               `  measure: order_count is count()\n` +
               `  measure: total_amount is amount.sum()\n` +
               `  join_one: users_source with user_id\n` +
               `}\n`,
            "extended/order_items.malloy":
               `import "../sources/order_items_source.malloy"\n` +
               `source: order_items is order_items_source extend { primary_key: order_id }\n`,
            "models/bridge.malloy":
               `import "../extended/order_items.malloy"\n` +
               parentModelStanza(
                  `join_one: order_items on Stage = "order_items"`,
               ),
            "published/explore/model.malloy":
               `import "../../models/bridge.malloy"\n` +
               `source: parent_model_explore is parent_model\n`,
         },
         dataFiles: {
            "parents.csv": PARENT_CSV,
            "order_items.csv": ORDER_ITEMS_CSV,
            "users.csv": USERS_CSV,
         },
      };
   }

   test("recurses on transitive-import join — joined fields appear under a join-qualified name", async () => {
      // The headline case. ``Created_at`` is reachable only via the
      // join graph (``parent_model → order_items``). It must show up as a
      // join-qualified ``name`` (``order_items.Created_at``) under the
      // top-level runnable ``source`` ``parent_model`` — the
      // runnable-source + dotted-name shape an indexer reads
      // via ``run: parent_model -> { group_by: `order_items.Created_at` }``.
      const rows = projectForSnapshot(
         await runExtraction(bridgeModelLayout(), packageRoot),
      );

      const createdAt = rows.find(
         (r) =>
            r.source === "parent_model" &&
            (r.name === "order_items.Created_at" ||
               r.name === "order_items.created_at"),
      );
      assert.ok(
         createdAt,
         "expected Created_at indexed as a join-qualified name " +
            "(order_items.Created_at) under the top-level runnable source " +
            "parent_model; got rows: " +
            JSON.stringify(rows.slice(0, 20)),
      );

      // Join entity itself should also still be present (the fix
      // adds recursive walking, doesn't replace existing extraction).
      const joinEntity = rows.find(
         (r) =>
            r.field_type === "join" &&
            r.join_target === "order_items" &&
            r.source === "parent_model",
      );
      assert.ok(
         joinEntity,
         "join entity for order_items should still be emitted",
      );
   });

   test("recurses two levels deep — joins-of-joins are walked too", async () => {
      // ``order_items_source`` has ``join_one: users_source with user_id``.
      // Once the walk recurses into ``parent_model.order_items``, it
      // must ALSO recurse into ``order_items``'s inner join to
      // ``users_source`` (also a non-top-level source), giving rows like:
      //   bridge_model.order_items.users_source | dimension | 16
      //   bridge_model.order_items.users_source | measure   | 3
      const rows = projectForSnapshot(
         await runExtraction(bridgeModelLayout(), packageRoot),
      );

      const secondLevel = rows.filter(
         (r) =>
            r.source === "parent_model" &&
            r.name.startsWith("order_items.users_source."),
      );
      assert.ok(
         secondLevel.length > 0,
         "expected second-level join-path names " +
            "(order_items.users_source.*) under the top-level runnable " +
            "source parent_model; got names: " +
            JSON.stringify(
               rows
                  .filter((r) => r.source === "parent_model")
                  .map((r) => r.name),
            ),
      );

      const userCount = secondLevel.find(
         (r) => r.name === "order_items.users_source.user_count",
      );
      assert.ok(
         userCount && userCount.field_type === "measure",
         "expected user_count measure indexed at the second-level join path",
      );
   });

   test("terminates cleanly on cycle-like mutual-join structures", async () => {
      // Defensive: a 3-level import chain whose inner file has
      // mutually-joined non-top-level sources. There can't be a true
      // structural cycle (Malloy errors on forward references at
      // compile, so structDef trees are finite DAGs), but the gate
      // still has to fire correctly. Asserts finite-time termination
      // and a known finite set of dotted paths.
      const layout: PackageLayout = {
         entryRelativePath: "entry.malloy",
         files: {
            "lib/inner.malloy":
               `source: inner_a_base is duckdb.table('a.csv') extend { primary_key: id }\n` +
               `source: inner_b is duckdb.table('b.csv') extend {\n` +
               `  primary_key: id\n` +
               `  measure: cnt_b is count()\n` +
               `  join_one: inner_a_base on a_id = inner_a_base.id\n` +
               `}\n` +
               `source: inner_a is inner_a_base extend {\n` +
               `  join_one: inner_b on b_id = inner_b.id\n` +
               `}\n`,
            "middle.malloy":
               `import "./lib/inner.malloy"\n` +
               `source: middle_outer is inner_a extend { measure: cnt_outer is count() }\n`,
            "entry.malloy":
               `import "./middle.malloy"\n` +
               `source: entry_explore is middle_outer\n`,
         },
         dataFiles: {
            "a.csv": "id,b_id,val\n1,1,alpha\n",
            "b.csv": "id,a_id,name\n1,1,one\n",
         },
      };

      const start = Date.now();
      const rows = projectForSnapshot(await runExtraction(layout, packageRoot));
      const elapsed = Date.now() - start;

      assert.ok(
         elapsed < 2000,
         `extraction should terminate quickly; took ${elapsed}ms`,
      );

      // Leaf (dimension/measure/column) entities — the only ones an indexer
      // ever queries for values — ALWAYS hang off a top-level
      // runnable source. Join entities keep their dotted source
      // (a host's join-edge topology depends on it), so we check the
      // two axes separately.
      const leafSources = Array.from(
         new Set(
            rows
               .filter((r) => r.field_type !== "join")
               .map((r) => r.source)
               .filter((s): s is string => !!s),
         ),
      ).sort();
      assert.deepStrictEqual(leafSources, ["entry_explore", "middle_outer"]);

      // Recursion depth lives in the join-qualified leaf NAMES. A finite
      // set of prefixes — first-level (``inner_b``) and second-level
      // (``inner_b.inner_a_base``), no third level since ``inner_a_base``
      // has no further joins — is what proves clean termination.
      const joinPaths = Array.from(
         new Set(
            rows
               .filter((r) => r.field_type !== "join" && r.name.includes("."))
               .map((r) => r.name.slice(0, r.name.lastIndexOf("."))),
         ),
      ).sort();
      assert.deepStrictEqual(joinPaths, ["inner_b", "inner_b.inner_a_base"]);
   });
});

describe("package-sanitization: regression guards for the recursion gate", () => {
   let packageRoot: string;
   beforeEach(async () => {
      packageRoot = await mkdtemp(path.join(tmpdir(), "pgate-"));
   });
   afterEach(async () => {
      await rm(packageRoot, { recursive: true, force: true });
   });

   test("gate skips when join target IS at top level (no over-indexing)", async () => {
      // Layout (c): entry directly imports ``order_items.malloy``.
      // ``order_items`` is in ``compiled.explores`` at top level, so
      // its fields are covered by the top-level walk. The gate must
      // NOT also recurse into the join — that would emit duplicate
      // rows under ``parent_model.order_items.*``.
      const layout: PackageLayout = {
         entryRelativePath: "published/explore/model.malloy",
         files: {
            "sources/order_items.malloy": ORDER_ITEMS_DECL,
            "published/explore/model.malloy":
               `import "../../sources/order_items.malloy"\n` +
               parentModelStanza(
                  `join_one: order_items on Stage = "order_items"`,
               ),
         },
         dataFiles: {
            "parents.csv": PARENT_CSV,
            "order_items.csv": ORDER_ITEMS_CSV,
         },
      };

      const rows = projectForSnapshot(await runExtraction(layout, packageRoot));
      // Over-indexing would show up as join-qualified names
      // (``order_items.<field>``) hanging off ``parent_model`` — NOT as
      // a dotted source, since leaf fields always use a top-level
      // runnable source. The join entity itself (``name === "order_items"``)
      // is expected; only recursed leaf/measure fields under it are the bug.
      const overIndexed = rows.filter(
         (r) =>
            r.source === "parent_model" &&
            r.name.startsWith("order_items.") &&
            r.field_type !== "join",
      );
      assert.strictEqual(
         overIndexed.length,
         0,
         "expected NO order_items.* join-qualified names under " +
            "parent_model when order_items is already a top-level source; " +
            "got: " +
            JSON.stringify(overIndexed),
      );
      // Sanity: order_items IS in the index as a top-level source.
      const topLevel = rows.filter((r) => r.source === "order_items");
      assert.ok(
         topLevel.length > 0,
         "expected order_items's own fields indexed at top level",
      );
   });

   test("gate fires for is_nested_index=true (alias + extend{}) — existing behaviour preserved", async () => {
      // The pre-existing recursion path: ``join_one: oi is X extend {...}``
      // had ``is_nested_index=true``, recursion already fired. Pin that
      // the fix didn't accidentally regress this branch — rows under
      // ``parent_model.oi.*`` should still appear.
      const layout: PackageLayout = {
         entryRelativePath: "model.malloy",
         files: {
            "model.malloy":
               ORDER_ITEMS_DECL +
               parentModelStanza(
                  `join_one: oi is order_items extend { measure: row_count is count() } ` +
                     `on order_id = oi.order_id`,
               ),
         },
         dataFiles: {
            "parents.csv": PARENT_CSV,
            "order_items.csv": ORDER_ITEMS_CSV,
         },
      };

      const rows = projectForSnapshot(await runExtraction(layout, packageRoot));
      const nestedRowCount = rows.find(
         (r) => r.source === "parent_model" && r.name === "oi.row_count",
      );
      assert.ok(
         nestedRowCount,
         "expected the extend{}-added measure as a join-qualified name " +
            "oi.row_count under the top-level runnable source parent_model " +
            "(the existing is_nested_index=true path)",
      );
   });

   test("direct conn.table() join (no separate source declaration) — existing recursion preserved", async () => {
      // ``join_one: oi is duckdb.table('...')`` was the other existing
      // ``is_nested_index=true`` case. Make sure it still walks the
      // table's columns under the dotted path.
      const layout: PackageLayout = {
         entryRelativePath: "model.malloy",
         files: {
            "model.malloy": parentModelStanza(
               `join_one: oi is duckdb.table('order_items.csv') on order_id = oi.order_id`,
            ),
         },
         dataFiles: {
            "parents.csv": PARENT_CSV,
            "order_items.csv": ORDER_ITEMS_CSV,
         },
      };

      const rows = projectForSnapshot(await runExtraction(layout, packageRoot));
      const createdAt = rows.find(
         (r) =>
            r.source === "parent_model" &&
            (r.name === "oi.Created_at" || r.name === "oi.created_at"),
      );
      assert.ok(
         createdAt,
         "expected Created_at as a join-qualified name oi.Created_at under " +
            "the top-level runnable source parent_model (direct-table join, " +
            "is_nested_index=true)",
      );
   });
});
