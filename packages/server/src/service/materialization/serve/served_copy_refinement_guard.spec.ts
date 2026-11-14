// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The guard that lets the served copy re-root a v1 `#@ persist` source
 * declared by refinement alone (`high_value is raw extend { … }`) on its own
 * table: only when its base chain applies nothing the re-root would drop. The
 * filter half is reached end to end (served_copy_refinement_root); the gate
 * half is pinned here, in isolation, since no fixture reaches it: a base
 * gated by `#(access_filter)` or `#(authorize)` but carrying no `where:` must
 * keep the source as written.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   type ModelDef,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { SourceMap } from "../plan/source_map";
import { refinesUnfilteredBase } from "./served_copy_serve";

let duckdb: DuckDBConnection;
beforeAll(() => {
   duckdb = new DuckDBConnection("duckdb", ":memory:");
});
afterAll(async () => {
   await duckdb.close();
});

const URL_OF = "file:///guard/m.malloy";

async function guardFor(text: string, source = "child"): Promise<boolean> {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[URL_OF, text]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const model = await runtime.loadModel(new URL(URL_OF)).getModel();
   const modelDef = (model as unknown as { _modelDef: ModelDef })._modelDef;
   const map = SourceMap.of(modelDef);
   return refinesUnfilteredBase(map.node(`${source}@${URL_OF}`), map);
}

const RAW = `source: raw is duckdb.sql("SELECT 1 AS org_id, 10 AS amount")`;

describe("refinesUnfilteredBase", () => {
   it("admits a refinement of a base that filters nothing and has no gate", async () => {
      expect(
         await guardFor(
            `${RAW}\nsource: child is raw extend { where: amount >= 5 }`,
         ),
      ).toBe(true);
   });

   it("refuses a base with a where: of its own", async () => {
      expect(
         await guardFor(
            `${RAW}\nsource: base is raw extend { where: amount > 1 }\nsource: child is base extend { where: amount >= 5 }`,
         ),
      ).toBe(false);
   });

   it("refuses a base gated by #(access_filter), though it carries no where:", async () => {
      expect(
         await guardFor(`##! experimental.givens
given: ORG :: number
${RAW}
#(access_filter) org_id = $ORG
source: base is raw extend {}
source: child is base extend { where: amount >= 5 }`),
      ).toBe(false);
   });

   it("refuses a base locked by #(authorize), though it carries no where:", async () => {
      expect(
         await guardFor(`##! experimental.givens
given: ROLE :: string
${RAW}
#(authorize) $ROLE = 'admin'
source: base is raw extend {}
source: child is base extend { where: amount >= 5 }`),
      ).toBe(false);
   });

   it("refuses a query-shaped source, whose base hop is a query", async () => {
      expect(
         await guardFor(`${RAW}\nsource: child is raw -> { group_by: org_id }`),
      ).toBe(false);
   });
});
