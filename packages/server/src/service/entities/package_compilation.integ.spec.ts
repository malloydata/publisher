// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @fileoverview Integration test for entity extraction over a NESTED-INDEX
 * join, compiling a real Malloy model against an in-process DuckDB connection.
 *
 * Pins the contract for fields reached through an inline-defined join
 * (e.g. `principals.people.primaryName`): a LEAF field must be stored as a
 * runnable source + join-qualified name — source="principals",
 * name="people.<field>" — never a non-runnable dotted `source`
 * ("principals.people") that would produce invalid Malloy
 * `run: principals.people -> { ... }`.
 *
 * Normal (non-joined) entities — top-level sources and the owning source's own
 * fields — must be completely unaffected, and the JOIN entity (field_type=
 * "join") must be left untouched, since only recursed leaf fields
 * (`!field.isExploreField()`) are rewritten.
 *
 * No external services — DuckDB runs in-process over CSVs written to a tmpdir.
 */

import { describe, test } from "bun:test";
import assert from "node:assert";
import { compileFixture } from "./test_support";

// A standalone top-level source `people`, plus `principals` which both has its
// own fields (a column, a measure, a dimension) AND an inline-defined
// (nested-index) join to a fresh `people` table. The inline join is what
// drives the recursion / dotted-source path; everything else is a "normal"
// entity whose shape must be preserved by the fix.
//
// `join_one: ... is duckdb.table(...)` → parseJoinCode marks is_nested_index
// =true, so getEntitiesFromExplore recurses into it; its leaf fields must not
// be reported under source="principals.people".
const MODEL = `
source: people is duckdb.table('people.csv')

source: principals is duckdb.table('principals.csv') extend {
  join_one: people is duckdb.table('people.csv') on nconst = people.nconst
  measure: principal_count is count()
  dimension: cat_upper is upper(category)
}

source: principals_extended is duckdb.table('principals.csv') extend {
  join_one: crew is people extend {
    dimension: shout is upper(primary_name)
  } on nconst = crew.nconst
}

source: principals_viewed is duckdb.table('principals.csv') extend {
  join_many: crew_view is people extend {} -> {
    group_by: crew_id is nconst
    aggregate: crew_count is count()
  } on nconst = crew_view.crew_id
}
`;

describe("extractEntitiesFromExplores (nested-index join, integration)", () => {
   // Compile MODEL against in-process DuckDB and return the extracted entities.
   async function compileEntities() {
      const [result] = await compileFixture({
         "people.csv": "nconst,primary_name\nnm1,Tom Hanks\nnm2,Meg Ryan\n",
         "principals.csv":
            "tconst,nconst,category\ntt1,nm1,actor\ntt2,nm2,actor\n",
         "model.malloy": MODEL,
      });
      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `compile failed: ${result.errorMessage}`,
      );
      return result.entities ?? [];
   }

   test("splits a joined leaf field into runnable source + join-qualified name", async () => {
      const entities = await compileEntities();

      // The leaf field reached through the join: runnable source + qualified
      // name. This is the pair that makes `run: principals -> { group_by:
      // value is `people`.`primary_name` }` valid for an indexer.
      const leaf = entities.find((e) => e.name === "people.primary_name");
      assert.ok(
         leaf,
         `expected a leaf entity name="people.primary_name"; got names: ${entities
            .map((e) => `${e.source}::${e.name}`)
            .join(", ")}`,
      );
      assert.strictEqual(leaf.source, "principals");

      // No LEAF field may keep a non-runnable dotted source. (The join entity
      // itself is emitted in the parent explore's loop with source="principals"
      // and field_type="join" — it is never recursed/dotted, and the fix leaves
      // it untouched; see the dedicated join-entity test below.)
      const dottedSourceLeaves = entities.filter(
         (e) =>
            e.field_type !== "join" &&
            typeof e.source === "string" &&
            e.source.includes("."),
      );
      assert.deepStrictEqual(
         dottedSourceLeaves.map((e) => `${e.source}::${e.name}`),
         [],
         "no leaf field should retain a dotted (non-runnable) source",
      );

      // The top-level field on the owning source is unchanged.
      const topLevel = entities.find(
         (e) => e.source === "principals" && e.name === "category",
      );
      assert.ok(topLevel, "top-level `category` field missing");
   }, 30_000);

   test("preserves columns of a standalone top-level source (no join)", async () => {
      // The `people` source is declared at top level (parentPath=null), so its
      // fields must keep source="people" and a plain, undotted leaf name — the
      // fix's `parentPath && ...` guard must not touch them.
      const entities = await compileEntities();

      const peopleCols = entities
         .filter((e) => e.source === "people")
         .map((e) => `${e.field_type}:${e.name}`)
         .sort();
      assert.deepStrictEqual(
         peopleCols,
         ["column:nconst", "column:primary_name"],
         `standalone top-level source columns changed: ${peopleCols.join(", ")}`,
      );
   }, 30_000);

   test("preserves the owning source's own column/measure/dimension fields", async () => {
      // Fields defined directly on `principals` (not reached through a join)
      // must keep source="principals" and their bare leaf name regardless of
      // entity type — column, measure, and dimension alike.
      const entities = await compileEntities();

      const own = (name: string) =>
         entities.find((e) => e.source === "principals" && e.name === name);

      const category = own("category");
      assert.ok(category, "principals.category (column) missing");
      assert.strictEqual(category.field_type, "column");

      const measure = own("principal_count");
      assert.ok(measure, "principals.principal_count (measure) missing");
      assert.strictEqual(measure.field_type, "measure");

      const dimension = own("cat_upper");
      assert.ok(dimension, "principals.cat_upper (dimension) missing");
      assert.strictEqual(dimension.field_type, "dimension");

      // None of these own fields carry a dotted/qualified name.
      for (const e of [category, measure, dimension]) {
         assert.ok(
            !e.name.includes("."),
            `own field name should not be qualified: ${e.name}`,
         );
      }
   }, 30_000);

   test("re-roots leaves of `extend {}` and `extend {} -> view` joins onto the owning source", async () => {
      // The leaves must land on the runnable root, not on "<root>.<alias>":
      // an indexer looks dimension values up by undotted source names only.
      const entities = await compileEntities();

      const leaves = (source: string) =>
         entities.filter((e) => e.source === source && e.field_type !== "join");
      assert.ok(
         leaves("principals_extended").some((e) => e.name === "crew.shout"),
         "extend {} join leaf missing from the owning source",
      );
      assert.ok(
         leaves("principals_viewed").some(
            (e) => e.name === "crew_view.crew_count",
         ),
         "extend {} -> view join leaf missing from the owning source",
      );
      assert.deepStrictEqual(
         entities
            .filter(
               (e) =>
                  e.field_type !== "join" &&
                  typeof e.source === "string" &&
                  e.source.includes("."),
            )
            .map((e) => `${e.source}::${e.name}`),
         [],
      );
   }, 30_000);

   test("leaves the JOIN entity itself untouched (field_type=join)", async () => {
      // The fix rewrites only recursed LEAF fields (`!field.isExploreField()`).
      // The join entity (field_type="join") is emitted in the parent loop and
      // must be unchanged: name is the join alias, source is the owning source.
      const entities = await compileEntities();

      const joinEntity = entities.find(
         (e) => e.field_type === "join" && e.source === "principals",
      );
      assert.ok(joinEntity, "join entity missing");
      assert.strictEqual(joinEntity.name, "people");
      assert.strictEqual(joinEntity.source, "principals");
   }, 30_000);
});
