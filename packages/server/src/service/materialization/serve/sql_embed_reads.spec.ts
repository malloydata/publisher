// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { SourceMap } from "../text/source_map";
import { materializationFixture } from "../../../test_helpers/materialization_fixture";
import { crossingSqlEmbeds, sqlEmbedReads } from "./sql_embed_reads";

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
   it("names each .sql() with a %{}, its connection and what it reads", async () => {
      const modelDef = await mz.compile(
         { "model.malloy": MODEL },
         { entry: "model.malloy" },
      );
      const map = SourceMap.of(modelDef);
      const id = (name: string) => map.sourceIDOf(name)!;
      const reads = sqlEmbedReads(modelDef);
      // The plain `.sql()` bases of a, b and c embed nothing; `ab` (and the
      // extension carrying its SQL) embeds `a` and `b`.
      expect(reads.length).toBeGreaterThan(0);
      for (const r of reads) {
         expect(r.connection).toBe("duckdb");
         expect(r.reaches.has(id("a"))).toBe(true);
         expect(r.reaches.has(id("b"))).toBe(true);
         expect(r.reaches.has(id("c"))).toBe(false);
      }
      expect(reads.map((r) => r.via)).toContain("ab");

      // On its own connection, nothing crosses; on another, `a` does.
      const none = () => false;
      expect(crossingSqlEmbeds(reads, id("a"), "duckdb", none)).toEqual([]);
      expect(
         crossingSqlEmbeds(reads, id("a"), "elsewhere", none).map((r) => r.via),
      ).toContain("ab");
      expect(crossingSqlEmbeds(reads, id("c"), "elsewhere", none)).toEqual([]);
      // A `.sql()` source the copy re-roots itself is replaced by its table.
      expect(
         crossingSqlEmbeds(reads, id("a"), "elsewhere", () => true),
      ).toEqual([]);
   });
});
