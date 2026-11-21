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
  dimension: is_big is amt > 1
  dimension: is_big_small is is_big and amt < 10
  dimension: is_books is category = 'books'
  dimension: is_big_book is is_big and is_books
  measure: nbig is count() { where: is_big }
  measure: nbooks is count() { where: is_books }
}

source: grants is duckdb.sql("SELECT 1 AS user_id, 2 AS opp_id, 'x' AS tag") -> { select: * } extend {
  dimension: tagged is tag = 'x'
}

source: opps is duckdb.sql("SELECT 2 AS opp_id, 0 AS restricted, 100 AS amount") -> { select: * } extend {
  join_one: g is grants on opp_id = g.opp_id
  dimension: visible is restricted = 0 or g.opp_id is not null
  dimension: visible_tagged is restricted = 0 or g.tagged
  measure: visible_amount is amount.sum() { where: visible }
  measure: hidden is count() { where: not visible }
}

source: v is t -> { select: id, category, amt, sb is category = 'books', big2 is amt > 1 } extend {
  dimension: vis2 is sb or amt > 1000
  dimension: ok2 is big2 or amt > 1000
  measure: nv is count() { where: vis2 }
}

source: v3 is v extend { dimension: vis3 is vis2 and id > 0 }

source: vstar is t -> { select: * }

source: jv is duckdb.sql("SELECT 1 AS oid") extend {
  join_one: vv is v on oid = vv.id
  dimension: jvis is vv.sb or oid > 1000
  dimension: jvis2 is vv.vis2
  dimension: jok is vv.big2 or oid > 1000
}

source: vg is t -> { group_by: category; aggregate: n is count() } extend {
  dimension: many is n > 2
}

source: vgn is t -> { group_by: amt; aggregate: n is count(), s is amt.sum() } extend {
  dimension: many is n > 2
}

source: vgb is t -> { group_by: amt; aggregate: nb } extend {
  dimension: many is nb > 2
}

source: tm is t extend {
  measure: mf is max(category) { where: category > 'M' }
  measure: mp is max(pick 'x' when category > 'M' else null)
  measure: np is sum(pick 1 when category > 'M' else 0)
  measure: nd is count(category)
  measure: np_ok is amt.sum() * 2 + c + count(amt)
}

source: tj is duckdb.sql("SELECT 1 AS oid, 'Books' AS cat") extend {
  join_one: tt is t on cat = tt.category
  join_one: tn is t on oid = tn.id
}

source: vr is t -> { group_by: category; order_by: category; calculate: rn is row_number() } extend {
  dimension: first is rn = 1
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

   it("reads a computed dimension that compares no string as comparing none, in `where:` and in a filtered measure", async () => {
      for (const q of [
         "run: t -> { where: is_big; select: id }",
         "run: t -> { where: not is_big_small; select: id }",
         "run: t -> { group_by: id; aggregate: nbig }",
         "run: t -> { group_by: id; aggregate: x is count() { where: is_big } }",
      ]) {
         const r = await reads(q);
         expect(r.comparesNoString, q).toBe(true);
         expect(r.scoped, q).toBe(false);
         expect(r.entryPaths, q).toEqual([]);
      }
   });

   it("reads a computed dimension through a join to a query-shaped source, as a grant table is read", async () => {
      const r = await reads(
         "run: opps -> { aggregate: visible_amount, hidden }",
      );
      expect(r.comparesNoString).toBe(true);
      expect(r.unresolved).toBe(false);
      expect(r.entryPaths).toEqual([]);
   });

   it("still reads a computed dimension that compares a string, however it is reached", async () => {
      for (const q of [
         "run: t -> { where: is_books; select: id }",
         "run: t -> { group_by: id; aggregate: nbooks }",
         "run: t -> { where: is_big_book; select: id }",
         "run: opps -> { where: visible_tagged; select: opp_id }",
      ]) {
         const r = await reads(q);
         expect(r.comparesNoString, q).toBe(false);
         expect(r.scoped, q).toBe(true);
      }
      const books = await reads("run: t -> { where: is_books; select: id }");
      expect(books.equalityOnly).toBe(true);
      expect(books.entryPaths).toContainEqual(["category"]);
      const tagged = await reads(
         "run: opps -> { where: visible_tagged; select: opp_id }",
      );
      expect(tagged.entryPaths).toContainEqual(["g", "tag"]);
   });

   it("reads a query-shaped source's output through the stage that computes it", async () => {
      // Each compares a string in a stage, and reads as one however it is named.
      for (const q of [
         "run: v -> { where: sb; select: id }",
         "run: v -> { where: vis2; select: id }",
         "run: v -> { aggregate: nv }",
         "run: v3 -> { where: vis3; select: id }",
         "run: jv -> { where: jvis; select: oid }",
         "run: jv -> { where: jvis2; select: oid }",
         "run: jv -> { where: vv.sb; select: oid }",
         "run: vg -> { where: many; select: category, n }",
         "run: vg -> { where: n > 2; select: category, n }",
         "run: vgb -> { where: many; select: amt }",
         "run: t -> { select: id, sb is category = 'books' } -> { where: sb; select: id }",
         "run: t -> { group_by: category; aggregate: c } -> { where: c > 1; select: c }",
         "run: t -> { group_by: id; nest: n is { select: sb is category = 'books' } -> { where: sb; select: sb } }",
      ]) {
         const r = await reads(q);
         expect(r.comparesNoString, q).toBe(false);
         expect(r.scoped, q).toBe(true);
      }
   });

   it("still reads a query-shaped source's output that compares no string as comparing none", async () => {
      for (const q of [
         "run: v -> { where: big2; select: id }",
         "run: v -> { where: ok2; select: id }",
         "run: v -> { where: amt > 1; select: id }",
         "run: vstar -> { where: amt > 1; select: id }",
         "run: jv -> { where: jok; select: oid }",
         "run: vgn -> { where: many and s > 1; select: amt }",
         "run: t -> { select: id, b is amt > 1 } -> { where: b; select: id }",
         "run: t -> { group_by: id; aggregate: c } -> { where: c > 1; select: c }",
         "run: t -> { group_by: id; nest: n is { select: b is amt > 1 } -> { where: b; select: b } }",
      ]) {
         const r = await reads(q);
         expect(r.comparesNoString, q).toBe(true);
         expect(r.scoped, q).toBe(false);
      }
   });

   it("reads a later stage's filter on a window function through what orders and partitions it", async () => {
      // The dimensional indexer's shard-boundary query: `rn` is a number, but
      // which row is 1 follows the order of `category`.
      for (const q of [
         "run: t -> { group_by: value is category order_by: value calculate: rn is row_number() } -> { where: rn = 1 or rn = 2 select: rn, value order_by: rn }",
         "run: t -> { group_by: category order_by: category calculate: rn is row_number() } -> { where: rn = 1 select: category }",
         "run: t -> { group_by: value is category, id calculate: r is rank() { partition_by: value order_by: id } } -> { where: r = 1 select: value }",
         "run: t -> { group_by: value is category order_by: value calculate: p is lag(value) } -> { where: p = 'books' select: value }",
      ]) {
         const r = await reads(q);
         expect(r.unresolved, q).toBe(false);
         expect(r.comparesNoString, q).toBe(false);
         expect(r.equalityOnly, q).toBe(false);
         expect(r.entryPaths, q).toContainEqual(["category"]);
         expect(r.entryPathsComplete, q).toBe(true);
      }
      // Ordered by a number alone, it compares no string.
      const byNumber = await reads(
         "run: t -> { group_by: id order_by: id calculate: rn is row_number() } -> { where: rn = 1 select: id }",
      );
      expect(byNumber.unresolved).toBe(false);
      expect(byNumber.entryPaths).toEqual([["id"]]);
      // Ordered by a string computed in the stage: no path from the entry point.
      const computed = await reads(
         "run: t -> { group_by: k is concat(category, '-x') order_by: k calculate: rn is row_number() } -> { where: rn = 1 select: k }",
      );
      expect(computed.unresolved).toBe(false);
      expect(computed.comparesNoString).toBe(false);
      expect(computed.entryPathsComplete).toBe(false);
   });

   it("still reads a window function it cannot follow as unread", async () => {
      // Ordered by an aggregate: what the count groups by is not followed.
      const r = await reads(
         "run: t -> { group_by: category; aggregate: n is count() order_by: n desc calculate: rn is row_number() } -> { where: rn = 1 select: category }",
      );
      expect(r.unresolved).toBe(true);
      expect(r.comparesNoString).toBe(false);
   });

   it("leaves a query-shaped source's own window function unread, as a gate reads it", async () => {
      // Only the caller's own stages are followed through a window function.
      const r = await reads("run: vr -> { where: first; select: category }");
      expect(r.unresolved).toBe(true);
      expect(r.comparesNoString).toBe(false);
   });

   it("reads a null test of a string column as comparing no string, and only that", async () => {
      for (const q of [
         "run: t -> { where: category is not null; select: id }",
         "run: t -> { where: category is null and amt > 1; select: id }",
         "run: t -> { aggregate: x is count() { where: category is not null } }",
      ]) {
         const r = await reads(q);
         expect(r.comparesNoString, q).toBe(true);
         expect(r.entryPaths, q).toEqual([]);
      }
      for (const q of [
         "run: t -> { where: category = ''; select: id }",
         "run: t -> { where: category is null or category = 'x'; select: id }",
         "run: t -> { where: nullif(category, 'x') is null; select: id }",
         "run: t -> { where: lc is null; select: id }",
         "run: t -> { where: late is null; select: id }",
      ]) {
         expect((await reads(q)).comparesNoString, q).toBe(false);
      }
   });

   it("reads a having: on a measure through its definition: its filters and what it aggregates", async () => {
      for (const q of [
         "run: tm -> { group_by: id; aggregate: n is count(); having: mf is null }",
         "run: tm -> { group_by: id; aggregate: n is count(); having: mp is null }",
         "run: tm -> { group_by: id; aggregate: n is count(); having: np > 0 }",
      ]) {
         const r = await reads(q);
         expect(r.comparesNoString, q).toBe(false);
         expect(r.scoped, q).toBe(true);
         expect(r.entryPaths, q).toContainEqual(["category"]);
      }
      // An ordering inside the measure: equality alone is not enough.
      expect(
         (
            await reads(
               "run: tm -> { group_by: id; aggregate: n is count(); having: np > 0 }",
            )
         ).equalityOnly,
      ).toBe(false);
      expect(
         (
            await reads(
               "run: tm -> { group_by: id; aggregate: n is count(); having: np_ok > 0 }",
            )
         ).comparesNoString,
      ).toBe(true);
   });

   it("reads a having: on an aggregate output through what it aggregates, never on trust", async () => {
      for (const q of [
         // An output naming a measure, and one computing it inline.
         "run: tm -> { group_by: id; aggregate: x is np; having: x > 0 }",
         "run: tm -> { group_by: id; aggregate: x is sum(pick 1 when category > 'M' else 0); having: x > 0 }",
         // A distinct count of a string counts what a collation tells apart.
         "run: tm -> { group_by: id; aggregate: n is count(); having: nd > 1 }",
         "run: tm -> { group_by: id; aggregate: x is count(category); having: x > 1 }",
         // `max` of a string orders it.
         "run: tm -> { group_by: id; aggregate: n is count(); having: mx is null }",
         // Through a join whose on: compares strings.
         "run: tj -> { group_by: oid; aggregate: x is tt.amt.sum(); having: x > 0 }",
      ]) {
         const r = await reads(q);
         expect(r.comparesNoString, q).toBe(false);
         expect(r.scoped, q).toBe(true);
      }
      for (const q of [
         "run: tm -> { group_by: id; aggregate: x is amt.sum(); having: x > 0 }",
         "run: tm -> { group_by: id; aggregate: x is count(amt); having: x > 1 }",
         "run: tj -> { group_by: oid; aggregate: x is tn.amt.sum(); having: x > 0 }",
      ]) {
         expect((await reads(q)).comparesNoString, q).toBe(true);
      }
   });

   it("reads a later stage's having: on an aggregate of an earlier stage's computed output back to it", async () => {
      for (const q of [
         "run: tm -> { select: id, big is pick 1 when category > 'M' else 0 } -> { group_by: id; aggregate: x is big.sum(); having: x > 0 }",
         "run: tm -> { group_by: id; aggregate: y is np } -> { group_by: id; aggregate: x is y.sum(); having: x > 0 }",
         "run: tm -> { select: id, big is pick 1 when category > 'M' else 0 } -> { select: id, b2 is big } -> { group_by: id; aggregate: x is b2.sum(); having: x > 0 }",
      ]) {
         const r = await reads(q);
         expect(r.comparesNoString, q).toBe(false);
         expect(r.scoped, q).toBe(true);
      }
      for (const q of [
         "run: tm -> { select: id, amt } -> { select: id, a2 is amt } -> { group_by: id; aggregate: x is a2.sum(); having: x > 0 }",
      ]) {
         expect((await reads(q)).comparesNoString, q).toBe(true);
      }
   });

   it("reads a field through a join whose on: compares strings as comparing one, a null test included", async () => {
      for (const q of [
         "run: tj -> { where: tt.category is null; select: oid }",
         "run: tj -> { where: tt.id is null; select: oid }",
         "run: tj -> { where: tt.amt > 1; select: oid }",
      ]) {
         expect((await reads(q)).comparesNoString, q).toBe(false);
      }
      for (const q of [
         "run: tj -> { where: tn.category is null; select: oid }",
         "run: tj -> { where: tn.amt > 1; select: oid }",
      ]) {
         expect((await reads(q)).comparesNoString, q).toBe(true);
      }
   });

   it("reads no filter at all as comparing no string", async () => {
      const r = await reads("run: t -> { group_by: category; aggregate: c }");
      expect(r.comparesNoString).toBe(true);
      expect(r.scoped).toBe(false);
   });
});
