// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { SourceMap } from "../text/source_map";
import { materializationFixture } from "../../../test_helpers/materialization_fixture";
import {
   conflictingSqlEmbeds,
   ownSqlEmbedReach,
   sqlEmbedReads,
} from "./sql_embed_reads";

const mz = materializationFixture();

const MODEL = `##! experimental.persistence
#@ persist name="a_rows"
source: a is duckdb.sql("select 1 as x") -> { select: x }
#@ persist name="b_rows"
source: b is duckdb.sql("select 2 as x") -> { select: x }
#@ persist name="c_rows"
source: c is duckdb.sql("select 3 as x") -> { select: x }
source: ab is duckdb.sql("""select * from %{ a -> { select: * } } union all select * from %{ b }""")
source: ab_ext is ab extend { dimension: y is x + 1 }
`;

describe("sqlEmbedReads", () => {
   it("names each .sql() with a %{} once, its connection and what its query and bare embeds read", async () => {
      const modelDef = await mz.compile(
         { "model.malloy": MODEL },
         { entry: "model.malloy" },
      );
      const map = SourceMap.of(modelDef);
      const id = (name: string) => map.sourceIDOf(name)!;
      // `ab`, and the extension carrying its SQL under its own name; the
      // model listed twice (an import's) reads each once.
      const reads = sqlEmbedReads([modelDef, modelDef, undefined]);
      expect(reads.map((r) => [r.via, r.connection]).sort()).toEqual([
         ["ab", "duckdb"],
         ["ab_ext", "duckdb"],
      ]);
      for (const r of reads) {
         expect(r.reaches.has(id("a"))).toBe(true);
         expect(r.reaches.has(id("b"))).toBe(false);
         expect(r.bareReaches.has(id("b"))).toBe(true);
         expect(r.reaches.has(id("c")) || r.bareReaches.has(id("c"))).toBe(
            false,
         );
      }

      const none = () => false;
      const conflicts = (sourceID: string, on: string, rerooted = none) =>
         conflictingSqlEmbeds(reads, sourceID, on, rerooted).map(
            (x) => x.conflict,
         );
      // A query embed: only on another connection.
      expect(conflicts(id("a"), "duckdb")).toEqual([]);
      expect(conflicts(id("a"), "elsewhere")).toEqual([
         "cross_connection",
         "cross_connection",
      ]);
      // A bare embed: on any connection.
      expect(conflicts(id("b"), "duckdb")).toEqual([
         "whole_source",
         "whole_source",
      ]);
      expect(conflicts(id("c"), "elsewhere")).toEqual([]);
      // A `.sql()` source the copy re-roots itself is replaced by its table.
      expect(conflicts(id("b"), "elsewhere", () => true)).toEqual([]);

      // The queried `.sql()` source's own embeds, extension included.
      for (const name of ["ab", "ab_ext"]) {
         const reach = ownSqlEmbedReach(modelDef.contents[name], modelDef);
         expect([name, reach.has(id("a")), reach.has(id("b"))]).toEqual([
            name,
            true,
            true,
         ]);
      }
      expect(ownSqlEmbedReach(modelDef.contents["c"], modelDef).size).toBe(0);
   });
});
