/**
 * Malloy recipes the analysis skills teach, compiled and run for real.
 *
 * A skill states a Malloy rule as a plain fact, and an agent copies it
 * exactly. A wrong fact compiles, runs and returns a wrong number with no
 * warning, so each recipe below runs against in-memory DuckDB (no services)
 * and asserts the numbers. The last block pins two compile failures the skills
 * warn about. They document what the pinned Malloy does today, so a Malloy
 * upgrade that changes either one fails here and the skill gets re-read.
 *
 * The skill text is read too, so a recipe cannot drift back to the wrong form
 * while the test keeps passing on its own copy.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, Runtime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";

const SKILLS_DIR = path.resolve(import.meta.dir, "../../../../../skills");

function skill(name: string): string {
   return fs.readFileSync(path.join(SKILLS_DIR, name, "SKILL.md"), "utf8");
}

// Two groups of three ordered steps. Group totals are 100 (A) and 200 (B),
// so a correct within-group share ends at 1.0 for both.
const STEPS = `source: steps is duckdb.sql("""
  select * from (values
    ('A', 1, 10), ('A', 2, 30), ('A', 3, 60),
    ('B', 1, 50), ('B', 2, 50), ('B', 3, 100)
  ) t(g, k, x)
""") extend { measure: tot is x.sum() }`;

// Six rows, four distinct regions, two null regions.
const WITH_NULLS = `source: with_nulls is duckdb.sql("""
  select * from (values
    (1, 'east'), (2, 'east'), (3, 'west'), (4, 'west'), (5, cast(null as varchar)), (6, cast(null as varchar))
  ) t(id, region)
""")`;

// Six rows, two distinct regions, no nulls, so the old recipe's answer
// (6 - 2 = 4) is the wrong one to expect.
const NO_NULLS = `source: no_nulls is duckdb.sql("""
  select * from (values
    (1, 'east'), (2, 'east'), (3, 'east'), (4, 'west'), (5, 'west'), (6, 'west')
  ) t(id, region)
""")`;

describe("analysis skill recipes compile and return the right numbers", () => {
   let duckdb: DuckDBConnection;
   let runtime: Runtime;

   async function run(model: string, query: string) {
      const result = await runtime.loadModel(model).loadQuery(query).run();
      return result.data.toObject() as Array<Record<string, unknown>>;
   }

   async function compileError(model: string, query: string): Promise<string> {
      try {
         await runtime.loadModel(model).loadQuery(query).run();
      } catch (error) {
         return error instanceof Error ? error.message : String(error);
      }
      throw new Error("expected a compile error, but the query compiled");
   }

   beforeAll(() => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      runtime = new Runtime({
         connections: new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         ),
      });
   });

   afterAll(async () => {
      await duckdb.close();
   });

   describe("cumulative share within a group", () => {
      it("with the denominator in aggregate: each group's curve ends at 1.0", async () => {
         const rows = await run(
            STEPS,
            `run: steps -> {
               group_by: g, k
               aggregate: tot, group_total is all(tot, g)
               calculate: cum_share is sum_cumulative(tot) { partition_by: g, order_by: k } / group_total
               order_by: g, k
            }`,
         );
         const share = (g: string) =>
            rows.filter((r) => r["g"] === g).map((r) => Number(r["cum_share"]));
         expect(share("A")).toEqual([0.1, 0.4, 1]);
         expect(share("B")).toEqual([0.25, 0.5, 1]);
      });

      it("with all(tot, g) inside calculate: it compiles but the curve overshoots 1.0", async () => {
         // The form the skill used to teach. Pinned so the skill's warning
         // ("compiles but returns a wrong denominator") stays true or gets
         // rewritten when Malloy changes.
         const rows = await run(
            STEPS,
            `run: steps -> {
               group_by: g, k
               aggregate: tot
               calculate: cum_share is sum_cumulative(tot) { partition_by: g, order_by: k } / all(tot, g)
               order_by: g, k
            }`,
         );
         const last = (g: string) =>
            Number(rows.filter((r) => r["g"] === g).at(-1)?.["cum_share"]);
         expect(last("A")).not.toBeCloseTo(1, 5);
         expect(last("B")).not.toBeCloseTo(1, 5);
      });

      it("a share of the grand total ends at 1.0 with sum_window", async () => {
         const rows = await run(
            STEPS,
            `run: steps -> {
               group_by: g, k
               aggregate: tot
               calculate: cum_share is sum_cumulative(tot) { order_by: g, k } / sum_window(tot)
               order_by: g, k
            }`,
         );
         expect(Number(rows.at(-1)?.["cum_share"])).toBeCloseTo(1, 10);
      });

      it("is the recipe the analysis skill teaches", () => {
         const text = skill("malloy-analysis");
         expect(text).toContain("/ group_total");
         expect(text).toContain(
            "`group_total is all(x, g)` is written in `aggregate:`",
         );
         expect(text).not.toContain(
            "} / all(x, g)` for a share within each group",
         );
      });
   });

   describe("counting rows with a null field", () => {
      const nullCount = `run: SRC -> { aggregate: rows is count(), null_rows is count() { where: region is null } }`;

      it("counts the null rows when there are nulls", async () => {
         const [row] = await run(
            WITH_NULLS,
            nullCount.replace("SRC", "with_nulls"),
         );
         expect(Number(row["rows"])).toBe(6);
         expect(Number(row["null_rows"])).toBe(2);
      });

      it("returns 0 when there are no nulls, even though values repeat", async () => {
         const [row] = await run(
            NO_NULLS,
            nullCount.replace("SRC", "no_nulls"),
         );
         expect(Number(row["null_rows"])).toBe(0);
      });

      it("count() - count(field) is NOT a null count (count(field) is distinct)", async () => {
         const [row] = await run(
            NO_NULLS,
            `run: no_nulls -> { aggregate: wrong is count() - count(region) }`,
         );
         // Six rows, no nulls, two distinct regions: 6 - 2 = 4, not 0.
         expect(Number(row["wrong"])).toBe(4);
      });

      it("is the recipe the analysis skill teaches", () => {
         const text = skill("malloy-analysis");
         expect(text).toContain("count() { where: the_field is null }");
         expect(text).not.toContain("`count() - count(the_field)` shows");
      });
   });

   describe("count(distinct field)", () => {
      it("is a compile error whose message says deprecated, not a parse error", async () => {
         const message = await compileError(
            NO_NULLS,
            `run: no_nulls -> { aggregate: n is count(distinct region) }`,
         );
         expect(message).toContain("deprecated");
         expect(message).toContain("count(expression)");
      });

      it("count(field) is already the distinct count", async () => {
         const [row] = await run(
            NO_NULLS,
            `run: no_nulls -> { aggregate: n is count(region) }`,
         );
         expect(Number(row["n"])).toBe(2);
      });
   });

   describe("an aggregate in where:", () => {
      it("fails with the message the gotchas skill quotes", async () => {
         const message = await compileError(
            NO_NULLS,
            `run: no_nulls -> { group_by: region, aggregate: n is count(), where: count() > 1 }`,
         );
         const quoted =
            "Aggregate expressions are not allowed in `where:`; use `having:`";
         expect(message).toContain(quoted);
         expect(skill("malloy-gotchas-queries")).toContain(quoted);
      });
   });

   describe("compile failures that today's Malloy reports unhelpfully", () => {
      // These document current behaviour. If either starts compiling, or the
      // message changes, update the skills that mention them.
      const DATED = `source: dated is duckdb.sql("""
         select 1 as id, cast('2025-03-04' as date) as order_date
      """)`;

      it("~ against a date literal fails; ? is the date form", async () => {
         const message = await compileError(
            DATED,
            `run: dated -> { where: order_date ~ @2025  aggregate: n is count() }`,
         );
         expect(message).toContain("mysterious error in range computation");
         const rows = await run(
            DATED,
            `run: dated -> { where: order_date ? @2025  aggregate: n is count() }`,
         );
         expect(Number(rows[0]["n"])).toBe(1);
      });

      it("joined.field.count() fails; count(joined.field) works", async () => {
         const model = `
            source: shipments is duckdb.sql("select 1 as order_id, 10 as shipment_id union all select 1, 11")
            source: orders is duckdb.sql("select 1 as order_id") extend {
               join_many: shipments on order_id = shipments.order_id
            }`;
         const message = await compileError(
            model,
            `run: orders -> { aggregate: n is shipments.shipment_id.count() }`,
         );
         expect(message).toContain("is not a source or join");
         const rows = await run(
            model,
            `run: orders -> { aggregate: n is count(shipments.shipment_id) }`,
         );
         expect(Number(rows[0]["n"])).toBe(2);
      });
   });
});
