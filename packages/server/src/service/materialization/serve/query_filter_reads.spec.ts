import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   type ModelMaterializer,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { queryFilterReadsOfQuery } from "./gate_reads";

const MODEL = `
source: t is duckdb.sql("SELECT 1 AS id, 'books' AS category, 2 AS amt") extend {
  primary_key: id
  measure: c is count()
  measure: nb is count() { where: category = 'books' }
  measure: nb2 is nb + 1
  measure: big is count() { where: amt > 1 }
  measure: mx is max(category)
  dimension: late is category > 'M'
  dimension: lc is lower(category)
  view: v is { where: category = 'books'; group_by: category; aggregate: c }
  view: vn is { where: amt > 1; group_by: category; aggregate: c }
}
`;

let conn: DuckDBConnection;
let model: ModelMaterializer;

beforeAll(() => {
   conn = new DuckDBConnection("duckdb", ":memory:");
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map()),
      connections: new FixedConnectionMap(
         new Map([["duckdb", conn]]),
         "duckdb",
      ),
   });
   model = runtime.loadModel(MODEL);
});

afterAll(async () => {
   await conn.close();
});

async function reads(
   query: string,
   options: { fromStage?: number; namedMeasures?: boolean } = {},
) {
   const r = await queryFilterReadsOfQuery(model.loadQuery(query), new Set(), {
      fromStage: options.fromStage ?? 0,
      namedMeasures: options.namedMeasures ?? true,
   });
   expect(r).toBeDefined();
   return r!;
}

describe("queryFilterReadsOfQuery: the caller's own filters", () => {
   it("reads a query's own string `where:` as an entry path, by equality", async () => {
      const r = await reads(
         "run: t -> { where: category = 'books'; select: * }",
      );
      expect(r.comparesNoString).toBe(false);
      expect(r.equalityOnly).toBe(true);
      expect(r.gated).toBe(false);
      expect(r.entryPaths).toEqual([["category"]]);
      expect(r.columns.flat()).toContain("category");
      expect(r.entryPathsComplete).toBe(true);
   });

   it("compares no string for a numeric `where:` and a numeric `having:`", async () => {
      const r = await reads(
         "run: t -> { where: amt > 1; having: c > 1; group_by: id; aggregate: c, big }",
      );
      expect(r.comparesNoString).toBe(true);
      expect(r.entryPaths).toEqual([]);
      expect(
         (
            await reads(
               "run: t -> { group_by: id; aggregate: n is count(); having: n > 1 }",
            )
         ).comparesNoString,
      ).toBe(true);
   });

   it("reads an ordering, a computed dimension and a string measure", async () => {
      expect(
         (await reads("run: t -> { where: category > 'M'; select: id }"))
            .equalityOnly,
      ).toBe(false);
      const late = await reads("run: t -> { where: late; select: id }");
      expect(late.comparesNoString).toBe(false);
      expect(late.equalityOnly).toBe(false);
      expect(late.entryPaths).toContainEqual(["category"]);
      const mx = await reads(
         "run: t -> { group_by: id; aggregate: mx; having: mx > 'M' }",
      );
      expect(mx.comparesNoString).toBe(false);
      expect(mx.entryPaths).toContainEqual(["category"]);
      const inline = await reads(
         "run: t -> { group_by: id; aggregate: m is max(category); having: m > 'M' }",
      );
      expect(inline.comparesNoString).toBe(false);
      expect(inline.entryPaths).toContainEqual(["category"]);
   });

   it("reads a filtered measure's filter, named, inline, through `having:` and through another measure", async () => {
      for (const q of [
         "run: t -> { group_by: id; aggregate: nb }",
         "run: t -> { group_by: id; aggregate: x is count() { where: category = 'books' } }",
         "run: t -> { group_by: id; aggregate: c; having: nb > 0 }",
         "run: t -> { group_by: id; aggregate: nb2 }",
      ]) {
         const r = await reads(q);
         expect(r.comparesNoString, q).toBe(false);
         expect(r.equalityOnly, q).toBe(true);
         expect(r.entryPaths, q).toContainEqual(["category"]);
      }
   });

   it("leaves a named measure's filter to a rollup's build, not an inline one", async () => {
      expect(
         (
            await reads("run: t -> { group_by: id; aggregate: nb }", {
               namedMeasures: false,
            })
         ).comparesNoString,
      ).toBe(true);
      expect(
         (
            await reads(
               "run: t -> { group_by: id; aggregate: x is count() { where: category = 'books' } }",
               { namedMeasures: false },
            )
         ).comparesNoString,
      ).toBe(false);
   });

   it("reads a view's filter and a nest's filter", async () => {
      expect((await reads("run: t -> v")).entryPaths).toEqual([["category"]]);
      expect((await reads("run: t -> vn")).comparesNoString).toBe(true);
      const nest = await reads(
         "run: t -> { group_by: id; nest: n is { where: category = 'books'; group_by: amt } }",
      );
      expect(nest.comparesNoString).toBe(false);
      expect(nest.entryPaths).toEqual([["category"]]);
   });

   it("reads a later stage's filter back to the entry point's column, and only the stages from `fromStage`", async () => {
      const q = "run: t -> v -> { where: category = 'x'; select: * }";
      const all = await reads(q);
      expect(all.entryPaths).toEqual([["category"], ["category"]]);
      const later = await reads(q, { fromStage: 1 });
      expect(later.comparesNoString).toBe(false);
      expect(later.entryPaths).toEqual([["category"]]);
      expect(later.columns.flat()).toContain("category");
      // A later stage's numeric filter, on an aggregate's output, compares no string.
      expect(
         (
            await reads("run: t -> v -> { where: c > 1; select: * }", {
               fromStage: 1,
            })
         ).comparesNoString,
      ).toBe(true);
      // A computed stage output has no path from the entry point.
      const computed = await reads(
         "run: t -> { group_by: k is concat(category, '-x') } -> { where: k = 'books-x'; select: * }",
         { fromStage: 1 },
      );
      expect(computed.comparesNoString).toBe(false);
      expect(computed.entryPathsComplete).toBe(false);
   });

   it("reads no filter at all as comparing no string", async () => {
      const r = await reads("run: t -> { group_by: category; aggregate: c }");
      expect(r.comparesNoString).toBe(true);
      expect(r.scoped).toBe(false);
   });
});
