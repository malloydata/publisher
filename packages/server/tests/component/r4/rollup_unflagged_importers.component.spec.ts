// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Campaign P1-20 (coverage gap G6), as a real package: a `#(rollup)` declared
 * in a file that carries the persistence flag, re-exported by an
 * `index.malloy` that does not, beside a third model that carries no flag and
 * no annotation and imports the declaring file. A plan discarded because a
 * model lacks the flag would answer every query live, equal to live, and go
 * unnoticed; so the plan must hold the rollup, a query through the published
 * `index.malloy` must be read from it, and every answer must equal live.
 *
 * Through the real load and bind (package_runner.ts). Live answers are taken
 * before the bind; each value is compared as a string. The rollup's table is
 * then tampered with a suffix on its grain, so an answer read from it is told
 * apart from live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { Row } from "../../../src/test_helpers/conformance/answer_difference";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";

const FILES: Record<string, string> = {
   "rollups.malloy": `##! experimental { persistence }
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, 'A', 10.5), (2, 'A', 20.25), (3, 'B', 30.0), (4, 'C', 7.75)
  ) AS t(id, category, amount)
""") extend {
  primary_key: id
  #(rollup) grain=["category"]
  measure: total is amount.sum()
}
`,
   "index.malloy": `import "rollups.malloy"
export { orders }
`,
   "third.malloy": `import "rollups.malloy"
source: big_orders is orders extend { where: amount > 10 }
source: plain is duckdb.sql("""SELECT 1 AS k UNION ALL SELECT 2""")
`,
};

const BY_CATEGORY =
   "run: orders -> { group_by: category; aggregate: total; order_by: category }";

/** Each query, and what answers it: the rollup, or live (a filter off its grain, a source it does not cover). */
const QUERIES: { model: string; malloy: string; servedBy: string }[] = [
   { model: "index.malloy", malloy: BY_CATEGORY, servedBy: "rollup" },
   { model: "rollups.malloy", malloy: BY_CATEGORY, servedBy: "rollup" },
   { model: "third.malloy", malloy: BY_CATEGORY, servedBy: "rollup" },
   {
      model: "third.malloy",
      malloy:
         "run: big_orders -> { group_by: category; aggregate: total; order_by: category }",
      servedBy: "live",
   },
   {
      model: "third.malloy",
      malloy: "run: plain -> { group_by: k; order_by: k }",
      servedBy: "live",
   },
];

/** A value as an exact string: never read back through a double. */
function exact(value: unknown): string {
   return typeof value === "object" && value !== null
      ? JSON.stringify(value)
      : String(value);
}

function asStrings(rows: Row[]): Record<string, string>[] {
   return rows.map((r) =>
      Object.fromEntries(Object.entries(r).map(([k, v]) => [k, exact(v)])),
   );
}

function answered(o: Outcome | undefined) {
   if (!o) throw new Error("no outcome");
   if ("error" in o) throw new Error(o.error);
   return o.answer;
}

describe("P1-20: a #(rollup) in a flagged file, re-exported by an unflagged index.malloy, beside an unflagged third model", () => {
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: LoadedFixture;
   const live: Outcome[] = [];

   beforeAll(async () => {
      restore = withSwitch("on");
      await freshLoadPool();
      loader = await loadFixture(FILES);
      for (const q of QUERIES) live.push(await loader.run(q.model, q.malloy));
      fixture = await loader.bind({ includeRollups: true });
   }, 300_000);

   afterAll(async () => {
      restore?.();
      await loader?.dispose();
      await freshLoadPool();
   });

   /** The rollup's plan entry. */
   function rollupEntry() {
      const rollups = Object.values(
         fixture.pkg.getBuildPlan()?.sources ?? {},
      ).filter((s) => s.origin === "rollup");
      expect(rollups).toHaveLength(1);
      return rollups[0];
   }

   it("plans and builds the rollup", () => {
      const entry = rollupEntry();
      expect(entry.modelPath).toBe("rollups.malloy");
      expect(fixture.buildErrors).toEqual({});
      expect(fixture.tables[entry.sourceEntityId]).toBeDefined();
   });

   for (const [i, q] of QUERIES.entries()) {
      it(`${q.model}: ${q.malloy}: answered ${q.servedBy === "live" ? "live" : "from the rollup"}, equal to live`, async () => {
         const l = answered(live[i]);
         expect(l.servedBy).toBe("live");
         expect(l.rows.length).toBeGreaterThan(0);
         const s = answered(await fixture.run(q.model, q.malloy));
         expect(s.servedBy).toBe(q.servedBy);
         if (q.servedBy === "rollup") expect(s.servedFrom).toBe("database");
         expect(asStrings(s.rows)).toEqual(asStrings(l.rows));
      });
   }

   it("reads the rollup's own table: tampered, every answer from it carries its suffix", async () => {
      const table = fixture.tables[rollupEntry().sourceEntityId];
      const duckdb = await fixture.pkg.getMalloyConnection("duckdb");
      await duckdb.runSQL(
         `UPDATE "${table}" SET category = category || '_rollup'`,
      );
      for (const [i, q] of QUERIES.entries()) {
         const l = answered(live[i]);
         const s = answered(await fixture.run(q.model, q.malloy));
         const suffix = q.servedBy === "rollup" ? "_rollup" : "";
         expect(s.servedBy).toBe(q.servedBy);
         expect(asStrings(s.rows)).toEqual(
            asStrings(
               l.rows.map((r) =>
                  "category" in r
                     ? { ...r, category: `${r.category}${suffix}` }
                     : r,
               ),
            ),
         );
      }
   });
});
