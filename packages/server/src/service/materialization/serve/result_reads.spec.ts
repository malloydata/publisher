import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   type ModelMaterializer,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { postgresJudged } from "./gate_collation";
import { type ResultReads, resultReadsOfQuery } from "./result_reads";

const MODEL = `
source: k is duckdb.sql("SELECT 'books' AS kname, 1 AS w") extend {
  primary_key: kname
}
source: t is duckdb.sql("SELECT 1 AS id, 'books' AS category, 2 AS amt, DATE '2020-01-01' AS d") extend {
  primary_key: id
  join_one: kk is k with category
  measure: c is count()
  measure: mx is max(category)
  measure: lo is min(lc)
  dimension: late is category > 'M'
  dimension: lc is lower(category)
  view: by_cat is { group_by: category; aggregate: c; order_by: category }
  view: by_n is { group_by: category; aggregate: c }
}
source: qs is t -> { group_by: category; aggregate: c }
source: qs_r is qs extend { rename: kind is category }
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
   options: { fromStage?: number; orderOnly?: number; stored?: string[] } = {},
): Promise<ResultReads> {
   const r = await resultReadsOfQuery(
      model.loadQuery(query),
      new Set(options.stored ?? []),
      {
         fromStage: options.fromStage ?? 0,
         ...(options.orderOnly !== undefined
            ? { orderOnly: options.orderOnly }
            : {}),
      },
   );
   expect(r).toBeDefined();
   return r!;
}

const paths = (r: ResultReads["ordered"]) =>
   r.entryPaths.map((p) => p.join(".")).sort();

describe("resultReadsOfQuery: what a result orders and groups strings by", () => {
   it("reads Malloy's implicit ordering: a group_by alone orders by it", async () => {
      const r = await reads("run: t -> { group_by: category }");
      expect(paths(r.ordered)).toEqual(["category"]);
      expect(paths(r.grouped)).toEqual(["category"]);
      expect(r.ordered.equalityOnly).toBe(false);
      expect(r.grouped.equalityOnly).toBe(true);
      expect(r.stringCollation).toBe("instance");
   });

   it("orders by the first measure when there is one, so a group_by is only grouped", async () => {
      const r = await reads("run: t -> { group_by: category; aggregate: c }");
      expect(r.ordered.comparesNoString).toBe(true);
      expect(paths(r.grouped)).toEqual(["category"]);
      const top = await reads(
         "run: t -> { group_by: category; aggregate: c; limit: 2 }",
      );
      expect(top.ordered.comparesNoString).toBe(true);
   });

   it("orders by the first time field, descending, before a string", async () => {
      const r = await reads("run: t -> { group_by: category, d }");
      expect(r.ordered.comparesNoString).toBe(true);
      expect(paths(r.grouped)).toEqual(["category"]);
   });

   it("reads an explicit order_by, by name and by position, and a top-N", async () => {
      expect(
         paths(
            (
               await reads(
                  "run: t -> { group_by: category; aggregate: c; order_by: category; limit: 2 }",
               )
            ).ordered,
         ),
      ).toEqual(["category"]);
      const r = await reads(
         "run: t -> { select: category, id; order_by: 1 desc }",
      );
      expect(paths(r.ordered)).toEqual(["category"]);
      // A select groups nothing.
      expect(r.grouped.comparesNoString).toBe(true);
   });

   it("reads nothing for a select with no ordering, or a numeric one", async () => {
      for (const q of [
         "run: t -> { select: category, id }",
         "run: t -> { select: category, id; order_by: id }",
         "run: t -> { group_by: amt; aggregate: c }",
         "run: t -> { aggregate: c }",
      ]) {
         const r = await reads(q);
         expect(r.ordered.comparesNoString, q).toBe(true);
         expect(r.grouped.comparesNoString, q).toBe(true);
      }
   });

   it("reads min and max of a string, inline, named or derived, as ordered", async () => {
      for (const q of [
         "run: t -> { aggregate: mx }",
         "run: t -> { aggregate: lo }",
         "run: t -> { aggregate: m is min(upper(category)) }",
         "run: t -> { group_by: amt; aggregate: m is category.max() }",
      ]) {
         expect(paths((await reads(q)).ordered), q).toEqual(["category"]);
      }
   });

   it("reads a distinct count as grouped, not ordered", async () => {
      const r = await reads("run: t -> { aggregate: n is count(category) }");
      expect(r.ordered.comparesNoString).toBe(true);
      expect(paths(r.grouped)).toEqual(["category"]);
   });

   it("reads a derived string as the column it is derived from", async () => {
      for (const q of [
         "run: t -> { group_by: lc }",
         "run: t -> { group_by: l is concat(category, '!') }",
         "run: t -> { group_by: s is substr(category, 1, 2) }",
         "run: t -> { group_by: p is pick 'x' when amt > 1 else category }",
         "run: t -> { group_by: g is greatest(category, 'm'); aggregate: c }",
      ]) {
         expect(paths((await reads(q)).ordered), q).toEqual(["category"]);
      }
   });

   it("reads a literal value that is ordered as compared under the database's default", async () => {
      const lit = await reads(
         "run: t -> { group_by: s is pick 'Hi' when amt > 1 else 'lo' }",
      );
      expect(lit.ordered.comparesNoString).toBe(false);
      expect(lit.ordered.entryPaths).toEqual([]);
      expect(lit.ordered.comparesOnlyColumns).toBe(false);
      const col = await reads("run: t -> { group_by: category }");
      expect(col.ordered.comparesOnlyColumns).toBe(true);
   });

   it("reads a range comparison inside a field as ordered, an equality as grouped", async () => {
      const late = await reads("run: t -> { group_by: late; aggregate: c }");
      expect(paths(late.ordered)).toEqual(["category"]);
      const eq = await reads(
         "run: t -> { group_by: b is category = 'books'; aggregate: c }",
      );
      expect(eq.ordered.comparesNoString).toBe(true);
      expect(paths(eq.grouped)).toEqual(["category"]);
      const pick = await reads(
         "run: t -> { group_by: p is category ? pick 'b' when 'books' else 'o'; aggregate: c }",
      );
      expect(pick.ordered.comparesNoString).toBe(true);
      expect(paths(pick.grouped)).toEqual(["category"]);
   });

   it("reads a window's own order_by as ordered and its partition_by as grouped", async () => {
      const own = await reads(
         "run: t -> { group_by: category; aggregate: c; calculate: r is rank() { order_by: category } }",
      );
      expect(paths(own.ordered)).toEqual(["category"]);
      const part = await reads(
         "run: t -> { group_by: category, amt; aggregate: c; calculate: r is rank() { partition_by: category; order_by: c desc } }",
      );
      expect(part.ordered.comparesNoString).toBe(true);
      expect(paths(part.grouped)).toEqual(["category"]);
   });

   it("reads a window that follows the stage's order through an intermediate stage", async () => {
      // An intermediate stage's order is dropped unless a window follows it.
      const dropped = await reads(
         "run: t -> { group_by: category } -> { aggregate: n is count() }",
      );
      expect(dropped.ordered.comparesNoString).toBe(true);
      const followed = await reads(
         "run: t -> { group_by: category; calculate: p is lag(category) } -> { aggregate: n is count() }",
      );
      expect(paths(followed.ordered)).toEqual(["category"]);
   });

   it("traces a later stage's ordering back to the entry point's column", async () => {
      const r = await reads(
         "run: t -> { group_by: category; aggregate: c } -> { select: category, c; order_by: category }",
      );
      expect(paths(r.ordered)).toEqual(["category"]);
      const byCount = await reads(
         "run: t -> { group_by: category; aggregate: c } -> { select: category, c; order_by: c desc }",
      );
      expect(byCount.ordered.comparesNoString).toBe(true);
   });

   it("reads a nest's ordering: its rows are an ordered array", async () => {
      const r = await reads(
         "run: t -> { group_by: amt; nest: n is { group_by: category } }",
      );
      expect(paths(r.ordered)).toEqual(["category"]);
   });

   it("reads a join's on and its keys as grouped, wherever the join is reached", async () => {
      const r = await reads("run: t -> { group_by: kk.w; aggregate: c }");
      expect(paths(r.grouped)).toEqual(["category", "kk.kname"]);
      const filtered = await reads(
         "run: t -> { where: kk.w > 0; aggregate: c }",
      );
      expect(paths(filtered.grouped)).toEqual(["category", "kk.kname"]);
   });

   it("on a stored result, judges only the ordering its read applies again, then the later stages", async () => {
      const exact = await reads("run: t -> by_cat", {
         fromStage: 1,
         orderOnly: 0,
      });
      expect(paths(exact.ordered)).toEqual(["category"]);
      expect(exact.grouped.comparesNoString).toBe(true);
      const byCount = await reads("run: t -> by_n", {
         fromStage: 1,
         orderOnly: 0,
      });
      expect(byCount.ordered.comparesNoString).toBe(true);
      expect(byCount.grouped.comparesNoString).toBe(true);
      const later = await reads(
         "run: t -> by_n -> { group_by: category; aggregate: s is c.sum() }",
         { fromStage: 1, orderOnly: 0 },
      );
      expect(later.ordered.comparesNoString).toBe(true);
      expect(paths(later.grouped)).toEqual(["category"]);
   });

   it("judges a query-shaped source's own query, unless its table is stored", async () => {
      const r = await reads("run: qs -> { select: category, c; order_by: c }");
      // The inner group_by reads its base, which has no path from `qs`.
      expect(r.grouped.comparesNoString).toBe(false);
      expect(r.grouped.entryPathsComplete).toBe(false);
      // Read whatever the outer query reads of it: its groups are its rows.
      const counted = await reads("run: qs -> { aggregate: k is count() }");
      expect(counted.grouped.comparesNoString).toBe(false);
      const prepared = (await model
         .loadQuery("run: qs -> { select: c }")
         .getPreparedQuery()) as unknown as {
         _modelDef: { contents: Record<string, { sourceID?: string }> };
      };
      const id = prepared._modelDef.contents.qs.sourceID!;
      const stored = await reads(
         "run: qs -> { select: category, c; order_by: c }",
         { stored: [id] },
      );
      expect(stored.grouped.comparesNoString).toBe(true);
      expect(stored.ordered.comparesNoString).toBe(true);
   });

   it("reads an index query's fields as ordered", async () => {
      const r = await reads("run: t -> { index: category, amt }");
      expect(r.ordered.unresolved).toBe(false);
      expect(paths(r.ordered)).toEqual(["category"]);
   });

   it("follows a rename of a stored query-shaped source's output", async () => {
      const prepared = (await model
         .loadQuery("run: qs -> { select: c }")
         .getPreparedQuery()) as unknown as {
         _modelDef: { contents: Record<string, { sourceID?: string }> };
      };
      const id = prepared._modelDef.contents.qs.sourceID!;
      const r = await reads("run: qs_r -> { group_by: kind }", {
         stored: [id],
      });
      expect(r.ordered.unresolved).toBe(false);
      expect(paths(r.ordered)).toEqual(["kind"]);
      const live = await reads("run: qs_r -> { group_by: kind }");
      expect(live.ordered.unresolved).toBe(false);
      expect(live.ordered.entryPathsComplete).toBe(false);
   });

   it("reads undefined from a compile it cannot read", async () => {
      expect(
         await resultReadsOfQuery(
            {
               getPreparedQuery: async () => {
                  throw new Error("no compile");
               },
            } as never,
            new Set(),
            { fromStage: 0 },
         ),
      ).toBeUndefined();
   });
});

describe("postgresJudged", () => {
   it("judges C, POSIX and the builtin provider binary", () => {
      expect(postgresJudged({ name: "C", det: true, prov: "c", cc: "C" })).toBe(
         "binary",
      );
      for (const name of ["pg_c_utf8", "ucs_basic", "pg_unicode_fast"]) {
         expect(postgresJudged({ name, det: true, prov: "b", cc: "C" })).toBe(
            "binary",
         );
      }
      expect(postgresJudged({ name: "default", dbp: "b", dbc: "C" })).toBe(
         "binary",
      );
   });

   it("judges libc and ICU locales deterministic, a nondeterministic one unknown", () => {
      expect(
         postgresJudged({
            name: "en_US",
            det: true,
            prov: "c",
            cc: "en_US.utf8",
         }),
      ).toBe("deterministic");
      expect(
         postgresJudged({ name: "und-x-icu", det: true, prov: "i", cc: "und" }),
      ).toBe("deterministic");
      expect(
         postgresJudged({ name: "default", dbp: "c", dbc: "en_US.utf8" }),
      ).toBe("deterministic");
      expect(postgresJudged({ name: "default", dbp: "i", dbc: "C" })).toBe(
         "deterministic",
      );
      expect(
         postgresJudged({ name: "ci", det: false, prov: "i", cc: "und" }),
      ).toBe("unknown");
      expect(postgresJudged(undefined)).toBe("unknown");
   });
});
