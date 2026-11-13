// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Wrong answers and misleading reports the conformance fixture
 * (ms2data/service#7098) found on main, each proven through the real package
 * path (conformance/package_harness.ts): a package is loaded, its plan built
 * as a build would build it, bound, and queried.
 *
 * Where a report is the subject, the proof is the SQL that ran: the tables a
 * path report says answered must be exactly the built tables the executed SQL
 * reads.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { unrecordedAudit } from "../../../src/service/access_record";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import { DATA_FILES, RAW_MODEL } from "./conformance/data";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   withSwitch,
} from "./conformance/package_harness";
import { rowDifference, type Row } from "./harness";

const HEADER = "##! experimental { persistence }";

/** Every statement DuckDB was handed since the last {@link takeSQL}. */
const statements: string[] = [];
const proto = DuckDBConnection.prototype as unknown as Record<
   string,
   (this: unknown, sql: string, ...rest: unknown[]) => unknown
>;
const originals = new Map<string, (typeof proto)[string]>();
for (const method of ["runSQL", "runSQLStream", "runRawSQL"]) {
   const original = proto[method];
   if (typeof original !== "function") continue;
   originals.set(method, original);
   proto[method] = function (sql: string, ...rest: unknown[]) {
      statements.push(sql);
      return original.call(this, sql, ...rest);
   };
}

function takeSQL(): string {
   const sql = statements
      .filter((s) => /\bSELECT\b/i.test(s) && !/^\s*DESCRIBE\b/i.test(s))
      .join("\n;\n");
   statements.length = 0;
   return sql;
}

interface Ran {
   rows: Row[];
   sql: string;
   servedBy: string;
   /** The tables the path report says answered (`used`), by tableId. */
   used: string[];
}

async function run(
   fixture: { pkg: LoadedFixture["pkg"] },
   modelPath: string,
   query: string,
): Promise<Ran> {
   const model = fixture.pkg.getModel(modelPath);
   if (!model) throw new Error(`${modelPath} did not load`);
   takeSQL();
   const result = await model.runQuery(
      unrecordedAudit(),
      { query } as never,
      QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
   );
   return {
      rows: result.compactResult as unknown as Row[],
      sql: takeSQL(),
      servedBy: result.path?.servedBy?.kind ?? "live",
      used: (result.path?.considered ?? [])
         .filter((c) => c.outcome === "used")
         .map((c) => c.tableId ?? ""),
   };
}

/** Each planned source's table, by its content address (the report's tableId). */
function tablesById(fixture: LoadedFixture): Map<string, string> {
   const out = new Map<string, string>();
   for (const s of Object.values(fixture.pkg.getBuildPlan()?.sources ?? {})) {
      out.set(s.sourceEntityId, (s.annotationFields?.name || s.name).trim());
   }
   return out;
}

/** Whether `sql` reads `table` (bare or quoted) in a FROM or JOIN. */
function reads(sql: string, table: string): boolean {
   const name = table.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
   return new RegExp(
      `\\b(FROM|JOIN)\\s+(${name}|"${name}")(\\s|$|\\))`,
      "i",
   ).test(sql);
}

/**
 * The report agrees with the SQL that ran: every built table the report says
 * answered is read by the SQL, and every built table the SQL reads is one the
 * report says answered.
 */
function expectReportMatchesSQL(fixture: LoadedFixture, ran: Ran): void {
   const tables = tablesById(fixture);
   const reported = new Set(
      ran.used.map((id) => tables.get(id)).filter((t): t is string => !!t),
   );
   const read = new Set([...tables.values()].filter((t) => reads(ran.sql, t)));
   expect([...reported].sort()).toEqual([...read].sort());
}

const withRaw = (files: Record<string, string>) => ({
   ...DATA_FILES,
   "raw.malloy": RAW_MODEL,
   ...files,
});

let restore: () => void = () => {};
beforeAll(async () => {
   // The fixture plans `#(materialize)` with the switch on; a pool another
   // spec file spawned would plan with its own settings.
   await freshLoadPool();
   restore = withSwitch("on");
});
afterAll(async () => {
   restore();
   await freshLoadPool();
   for (const [method, original] of originals) proto[method] = original;
});

/**
 * Load `files`, answer each query live, then build and bind the package as a
 * build would and hand it back, with the live rows by query key.
 */
async function loadBuilt(
   files: Record<string, string>,
   queries: Record<string, { model: string; query: string }>,
): Promise<{ fixture: LoadedFixture; live: Record<string, Row[]> }> {
   const loader = await loadFixture(withRaw(files));
   try {
      const live: Record<string, Row[]> = {};
      for (const [key, { model, query }] of Object.entries(queries)) {
         const outcome = await loader.run(model, query);
         if ("error" in outcome)
            throw new Error(`live ${key}: ${outcome.error}`);
         live[key] = outcome.answer.rows;
      }
      return { fixture: await loader.bind(), live };
   } catch (error) {
      await loader.dispose();
      throw error;
   }
}

function codes(fixture: LoadedFixture, code: string) {
   return fixture.warnings.filter((w) => w.code === code);
}

const SUMMARY_QUERY =
   "run: summary -> { group_by: region; aggregate: t is total.sum() }";

describe("the path report names exactly the tables the SQL read", () => {
   it("a colocated #(materialize) source is not built, reports live and reads the warehouse", async () => {
      const { fixture, live } = await loadBuilt(
         {
            "m.malloy": `import "raw.malloy"

#(materialize) name="summary_tbl"
source: summary is orders_raw -> {
  group_by: region, status
  aggregate: total is amount.sum()
}
`,
         },
         { q: { model: "m.malloy", query: SUMMARY_QUERY } },
      );
      try {
         // Not built until the served copy can serve it: a coded warning
         // says why, and no table is written (decided 2026-11-12).
         expect(fixture.built).toEqual([]);
         expect(
            codes(fixture, "materialize-not-built").map((w) => w.message),
         ).toEqual([
            expect.stringContaining(
               "colocated #(materialize) is not served yet; use storage= or #@ persist",
            ),
         ]);
         const ran = await run(fixture, "m.malloy", SUMMARY_QUERY);
         expect(rowDifference(live.q, ran.rows)).toBeUndefined();
         // Malloy never substitutes a #(materialize) source (it carries no
         // #@ persist), so the warehouse answered and the report says so.
         expect(reads(ran.sql, "summary_tbl")).toBe(false);
         expect(ran.servedBy).toBe("live");
         expectReportMatchesSQL(fixture, ran);
      } finally {
         await fixture.dispose();
      }
   });

   it("a colocated #@ -persist reader recomputes live and is reported live; its sibling reads the table and is reported", async () => {
      const queries = {
         reader: {
            model: "m.malloy",
            query: "run: summary_fresh -> { group_by: region; aggregate: grand }",
         },
         downstream: {
            model: "m.malloy",
            query: "run: summary_fresh_rollup -> { group_by: region; aggregate: t is g.sum() }",
         },
         sibling: {
            model: "m.malloy",
            query: "run: summary_kept -> { group_by: region; aggregate: grand }",
         },
      };
      const { fixture, live } = await loadBuilt(
         {
            "m.malloy": `${HEADER}
import "raw.malloy"

#@ persist name="summary"
source: summary is orders_raw -> {
  group_by: region, status
  aggregate: total is amount.sum()
}

#@ -persist
source: summary_fresh is summary extend {
  measure: grand is total.sum()
}

source: summary_fresh_rollup is summary_fresh -> {
  group_by: region
  aggregate: g is grand
}

source: summary_kept is summary extend {
  measure: grand is total.sum()
}
`,
         },
         queries,
      );
      try {
         for (const key of ["reader", "downstream"] as const) {
            const ran = await run(fixture, "m.malloy", queries[key].query);
            expect(rowDifference(live[key], ran.rows)).toBeUndefined();
            expect(reads(ran.sql, "summary")).toBe(false);
            expect(ran.servedBy).toBe("live");
            expectReportMatchesSQL(fixture, ran);
         }
         const sibling = await run(fixture, "m.malloy", queries.sibling.query);
         expect(rowDifference(live.sibling, sibling.rows)).toBeUndefined();
         expect(reads(sibling.sql, "summary")).toBe(true);
         expect(sibling.servedBy).toBe("materialized");
         expectReportMatchesSQL(fixture, sibling);
      } finally {
         await fixture.dispose();
      }
   });
});

describe("a model without the persistence flag beside a bound colocated table", () => {
   it("still answers, live", async () => {
      const queries = {
         a: { model: "a.malloy", query: SUMMARY_QUERY },
         b: { model: "b.malloy", query: "run: plain -> { aggregate: c }" },
      };
      const { fixture, live } = await loadBuilt(
         {
            "a.malloy": `${HEADER}
import "raw.malloy"

#@ persist name="summary_tbl"
source: summary is orders_raw -> {
  group_by: region, status
  aggregate: total is amount.sum()
}
`,
            "b.malloy": `import "raw.malloy"
source: plain is orders_raw extend { measure: c is count() }
`,
         },
         queries,
      );
      try {
         const a = await run(fixture, "a.malloy", queries.a.query);
         expect(reads(a.sql, "summary_tbl")).toBe(true);
         expectReportMatchesSQL(fixture, a);
         // Before: "A non-empty `buildManifest` was supplied, but the model is
         // missing `##! experimental.persistence`" on every query.
         const b = await run(fixture, "b.malloy", queries.b.query);
         expect(rowDifference(live.b, b.rows)).toBeUndefined();
         expect(b.servedBy).toBe("live");
      } finally {
         await fixture.dispose();
      }
   });
});

describe("a colocated table named by a reserved word", () => {
   it('is read quoted (name="order") and answers equal to live', async () => {
      const query =
         "run: `order` -> { group_by: `select`, `order`; aggregate: c is `count`.sum(), `group` }";
      const { fixture, live } = await loadBuilt(
         {
            "m.malloy": `${HEADER}
import "raw.malloy"

#@ persist name="order"
source: \`order\` is orders_raw -> {
  group_by: \`order\`, \`select\` is status
  aggregate: \`count\` is count(), amt is amount.sum()
} extend {
  measure: \`group\` is amt.sum()
}
`,
         },
         { q: { model: "m.malloy", query } },
      );
      try {
         // Before: Parser Error: syntax error at or near "order".
         const ran = await run(fixture, "m.malloy", query);
         expect(rowDifference(live.q, ran.rows)).toBeUndefined();
         expect(ran.sql).toContain('FROM "order"');
         expect(ran.servedBy).toBe("materialized");
         expectReportMatchesSQL(fixture, ran);
      } finally {
         await fixture.dispose();
      }
   });
});

/** Two models, each with a source `summary` over different rows. */
function sameNamed(annotation: string, header = HEADER) {
   const model = (status: string) => `${header}
import "raw.malloy"

${annotation}
source: summary is orders_raw -> {
  where: status = '${status}'
  group_by: region
  aggregate: total is amount.sum()
}
`;
   return { "a.malloy": model("placed"), "b.malloy": model("shipped") };
}

const SAME_NAMED_QUERIES = {
   a: { model: "a.malloy", query: SUMMARY_QUERY },
   b: { model: "b.malloy", query: SUMMARY_QUERY },
};

describe("same-named sources in two models (#6644)", () => {
   for (const [tier, annotation] of [
      ["colocated", "#@ persist"],
      ["storage", '#@ persist storage="lake"'],
   ] as const) {
      it(`v1 ${tier} with no name=: neither model reads the other's rows, both answer live`, async () => {
         const { fixture, live } = await loadBuilt(
            sameNamed(annotation),
            SAME_NAMED_QUERIES,
         );
         try {
            // Both still build, into the one table; the later overwrote it.
            expect(fixture.built.map((b) => b.name)).toEqual([
               "summary",
               "summary",
            ]);
            for (const key of ["a", "b"] as const) {
               const ran = await run(
                  fixture,
                  SAME_NAMED_QUERIES[key].model,
                  SUMMARY_QUERY,
               );
               // Before: a.malloy answered with b.malloy's rows.
               expect(rowDifference(live[key], ran.rows)).toBeUndefined();
               expect(ran.servedBy).toBe("live");
               expectReportMatchesSQL(fixture, ran);
            }
            expect(fixture.verdicts).toEqual([
               "summary:live:refused",
               "summary:live:refused",
            ]);
            const warning = fixture.warnings.find((w) =>
               w.message?.includes('same materialized table "summary"'),
            );
            expect(warning?.message).toContain("answered live");
         } finally {
            await fixture.dispose();
         }
      });
   }

   it("#(materialize) with no name= is not built, says to add name=, and both answer live", async () => {
      const { fixture, live } = await loadBuilt(
         sameNamed('#(materialize) storage="lake"', ""),
         SAME_NAMED_QUERIES,
      );
      try {
         expect(fixture.built).toEqual([]);
         const refused = codes(fixture, "materialize-not-built");
         expect(refused.map((w) => w.subject)).toEqual(["summary", "summary"]);
         for (const w of refused) expect(w.message).toContain("name=");
         for (const key of ["a", "b"] as const) {
            const ran = await run(
               fixture,
               SAME_NAMED_QUERIES[key].model,
               SUMMARY_QUERY,
            );
            expect(rowDifference(live[key], ran.rows)).toBeUndefined();
            expect(ran.servedBy).toBe("live");
         }
      } finally {
         await fixture.dispose();
      }
   });

   it("#(materialize) whose default name collides with a v1 table is refused; the v1 source still serves", async () => {
      const files = {
         "a.malloy": `${HEADER}
import "raw.malloy"

#@ persist name="summary" storage="lake"
source: placed is orders_raw -> {
  where: status = 'placed'
  group_by: region
  aggregate: total is amount.sum()
}
`,
         "b.malloy": `import "raw.malloy"

#(materialize) storage="lake"
source: summary is orders_raw -> {
  where: status = 'shipped'
  group_by: region
  aggregate: total is amount.sum()
}
`,
      };
      const queries = {
         a: {
            model: "a.malloy",
            query: "run: placed -> { group_by: region; aggregate: t is total.sum() }",
         },
         b: { model: "b.malloy", query: SUMMARY_QUERY },
      };
      const { fixture, live } = await loadBuilt(files, queries);
      try {
         expect(fixture.built).toEqual([{ name: "placed", tier: "storage" }]);
         const refused = codes(fixture, "materialize-not-built");
         expect(refused).toHaveLength(1);
         expect(refused[0].message).toContain('"placed" (a.malloy)');
         for (const key of ["a", "b"] as const) {
            const ran = await run(
               fixture,
               queries[key].model,
               queries[key].query,
            );
            expect(rowDifference(live[key], ran.rows)).toBeUndefined();
         }
         expect(fixture.verdicts).toEqual(["placed:serves"]);
      } finally {
         await fixture.dispose();
      }
   });

   it("#(materialize) with distinct name=s builds both and serves each its own rows", async () => {
      const files = {
         "a.malloy": sameNamed(
            '#(materialize) storage="lake" name="s_placed"',
            "",
         )["a.malloy"],
         "b.malloy": sameNamed(
            '#(materialize) storage="lake" name="s_shipped"',
            "",
         )["b.malloy"],
      };
      const { fixture, live } = await loadBuilt(files, SAME_NAMED_QUERIES);
      try {
         expect(codes(fixture, "materialize-not-built")).toEqual([]);
         for (const key of ["a", "b"] as const) {
            const ran = await run(
               fixture,
               SAME_NAMED_QUERIES[key].model,
               SUMMARY_QUERY,
            );
            expect(rowDifference(live[key], ran.rows)).toBeUndefined();
            expect(ran.servedBy).toBe("materialized");
         }
      } finally {
         await fixture.dispose();
      }
   });
   it("#(materialize) with equal explicit name=s is refused on both, naming the other, and both answer live", async () => {
      const { fixture, live } = await loadBuilt(
         sameNamed('#(materialize) storage="lake" name="s_dup"', ""),
         SAME_NAMED_QUERIES,
      );
      try {
         expect(fixture.built).toEqual([]);
         const refused = codes(fixture, "materialize-not-built");
         expect(refused.map((w) => w.subject)).toEqual(["summary", "summary"]);
         for (const w of refused) {
            expect(w.message).toContain("name= `s_dup`");
            expect(w.message).toContain("distinct name=");
         }
         expect(refused.map((w) => w.message).join("\n")).toContain(
            '"summary" (a.malloy)',
         );
         expect(refused.map((w) => w.message).join("\n")).toContain(
            '"summary" (b.malloy)',
         );
         for (const key of ["a", "b"] as const) {
            const ran = await run(
               fixture,
               SAME_NAMED_QUERIES[key].model,
               SUMMARY_QUERY,
            );
            expect(rowDifference(live[key], ran.rows)).toBeUndefined();
            expect(ran.servedBy).toBe("live");
         }
      } finally {
         await fixture.dispose();
      }
   });

   it("v1 sources with one table and the SAME definition share it legitimately: built, served, no collision", async () => {
      // One content address on one table is one table, not a collision: two
      // models persisting identical text read identical rows.
      const same = sameNamed('#@ persist storage="lake" name="s_same"');
      const files = {
         "a.malloy": same["a.malloy"],
         "b.malloy": same["a.malloy"],
      };
      const { fixture, live } = await loadBuilt(files, SAME_NAMED_QUERIES);
      try {
         expect(
            fixture.warnings.filter((w) =>
               w.message?.includes("same materialized table"),
            ),
         ).toEqual([]);
         expect(fixture.verdicts.every((v) => v.endsWith(":serves"))).toBe(
            true,
         );
         for (const key of ["a", "b"] as const) {
            const ran = await run(
               fixture,
               SAME_NAMED_QUERIES[key].model,
               SUMMARY_QUERY,
            );
            expect(rowDifference(live[key], ran.rows)).toBeUndefined();
            expect(ran.servedBy).toBe("materialized");
         }
      } finally {
         await fixture.dispose();
      }
   });
});

describe("#(materialize) on a source that inherits #@ persist", () => {
   it("is refused, naming the inherited annotation, and leaves the v1 plan whole", async () => {
      const query = "run: b -> { group_by: region; aggregate: grand }";
      const { fixture, live } = await loadBuilt(
         {
            "m.malloy": `${HEADER}
import "raw.malloy"

#@ persist name="a_tbl" storage="lake"
source: a is orders_raw -> {
  group_by: region, status
  aggregate: total is amount.sum()
}

#(materialize) storage="lake"
source: b is a extend {
  measure: grand is total.sum()
}
`,
         },
         { q: { model: "m.malloy", query } },
      );
      try {
         const refused = codes(fixture, "materialize-not-built");
         expect(refused).toHaveLength(1);
         expect(refused[0].subject).toBe("b");
         expect(refused[0].message).toContain("inherits #@ persist from `a`");
         // No source reads itself in the build graph.
         const plan = fixture.pkg.getBuildPlan();
         for (const graph of plan?.graphs ?? []) {
            for (const level of graph.nodes) {
               for (const node of level) {
                  expect(node.dependsOn).not.toContain(node.sourceID);
               }
            }
         }
         // b is a's table, as v1 has it.
         const b = Object.values(plan?.sources ?? {}).find(
            (s) => s.name === "b",
         );
         expect(b?.annotationFields?.name).toBe("a_tbl");
         const ran = await run(fixture, "m.malloy", query);
         expect(rowDifference(live.q, ran.rows)).toBeUndefined();
      } finally {
         await fixture.dispose();
      }
   });
});

describe("serve verdicts for #(materialize)", () => {
   it("read serves where the served copy answers, and live where it cannot", async () => {
      const { fixture } = await loadBuilt(
         {
            "m.malloy": `import "raw.malloy"

#(materialize) storage="lake"
source: summary is orders_raw -> {
  group_by: region, status
  aggregate: total is amount.sum()
}

#(materialize) name="colocated_tbl"
source: colocated is orders_raw -> {
  group_by: region
  aggregate: total is amount.sum()
}
`,
         },
         {},
      );
      try {
         // Before: "summary:live:shadowed" although the served copy answers it.
         // A colocated one is not planned at all (`materialize-not-built`).
         expect(fixture.verdicts.sort()).toEqual(["summary:serves"]);
         expect(
            codes(fixture, "materialize-not-built").map((w) => w.subject),
         ).toEqual(["colocated"]);
         const ran = await run(fixture, "m.malloy", SUMMARY_QUERY);
         expect(ran.servedBy).toBe("materialized");
         const colocated = await run(
            fixture,
            "m.malloy",
            "run: colocated -> { group_by: region; aggregate: t is total.sum() }",
         );
         expect(colocated.servedBy).toBe("live");
      } finally {
         await fixture.dispose();
      }
   });
});
