// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, test } from "bun:test";
import assert from "node:assert";
import type { Explore } from "@malloydata/malloy";
import { Runtime, InMemoryURLReader } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { serializeExploreToSourceInfo } from "./source_info_serializer";

// Compile-backed (real Malloy): the top-level SourceInfo annotations must be
// the ones genuinely declared on the source. Malloy copies a base's blockNotes
// verbatim onto an extending source that declares no annotation of its own, so
// without the location filter a childless-extend source would inherit the
// base's #(doc) into its summary/description (an indexer describes a source
// from its SourceInfo). Mirrors the extractDocsFromExplore test in
// package_sanitization.spec.ts.
describe("serializeExploreToSourceInfo — source #(doc) inheritance (compile-backed)", () => {
   // The `internal.malloy` URL makes extractCodeFromEntity (reached via
   // inferType) read code from the docString we pass instead of from disk.
   const MODEL_URL = "file:///internal.malloy";

   async function sourceInfoByName(model: string, names: string[]) {
      const conn = new DuckDBConnection("duckdb");
      try {
         const runtime = new Runtime({
            urlReader: new InMemoryURLReader(new Map([[MODEL_URL, model]])),
            connection: conn,
         });
         await conn.runRawSQL("CREATE OR REPLACE TABLE t AS SELECT 1 as id");
         const compiled = await runtime.getModel(new URL(MODEL_URL));
         const out: Record<
            string,
            ReturnType<typeof serializeExploreToSourceInfo>
         > = {};
         for (const n of names) {
            out[n] = serializeExploreToSourceInfo(
               compiled.getExploreByName(n) as unknown as Explore,
               model,
            );
         }
         return out;
      } finally {
         await conn.close();
      }
   }

   test("child with no own #(doc) does NOT inherit the base's; own doc still works", async () => {
      const model = `
#(doc) Base source doc.
source: s_base is duckdb.table('t') extend { dimension: d is id }

source: s_child is s_base extend { dimension: d2 is id }

#(doc) Own child doc.
source: s_child_doc is s_base extend { dimension: d3 is id }`;
      const infos = await sourceInfoByName(model, [
         "s_base",
         "s_child",
         "s_child_doc",
      ]);
      assert.deepStrictEqual(
         infos.s_base!.annotations.map((a) => a.value),
         ["#(doc) Base source doc."],
      );
      // no inheritance through extend
      assert.deepStrictEqual(infos.s_child!.annotations, []);
      assert.deepStrictEqual(
         infos.s_child_doc!.annotations.map((a) => a.value),
         ["#(doc) Own child doc."],
      );
   });

   test("field-level docs still travel with inherited fields", async () => {
      // The location filter applies to the SOURCE-level annotations only: a
      // field inherited unchanged legitimately keeps the doc declared on it.
      const model = `
source: f_base is duckdb.table('t') extend {
   #(doc) Dimension doc.
   dimension: d is id
}

source: f_child is f_base extend { dimension: d2 is id }`;
      const infos = await sourceInfoByName(model, ["f_child"]);
      const d = infos.f_child!.schema.fields.find((f) => f.name === "d");
      assert.deepStrictEqual(
         d?.annotations?.map((a) => a.value),
         ["#(doc) Dimension doc."],
      );
   });
});
