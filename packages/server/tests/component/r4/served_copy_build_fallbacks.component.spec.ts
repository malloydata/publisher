// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for the build side's fallbacks staying per source (found in the review
 * of #1678):
 *
 * - An `inline=` join whose rows read a given is refused for that source
 *   alone (`materialize-not-built`, P9's check from #1684): its stored
 *   columns would hold one caller's rows, and its uniqueness check would read
 *   the given, which fails the whole companion when the given has no default. It answers live, equal to
 *   live for each caller; every other source in the model is built and
 *   served.
 * - An extension table whose chained build does not compile over its
 *   parent's build source builds over the raw chain (the same rows), and
 *   that one alone: its sibling still builds over the parent's stored table.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'east', 10.5, 'r1'), (2, 'west', 20.0, 'r2'), (3, 'east', 5.5, 'r3'),
      (4, 'west', 11.0, 'r4'), (5, 'east', 3.0, NULL)
    ) AS t(id, region, amt, rid)`,
   `CREATE TABLE rg AS SELECT * FROM (VALUES
      ('r1', 'east', 'North'), ('r2', 'west', 'South'), ('r3', 'east', 'Mid'),
      ('r4', 'west', 'Far')
    ) AS t(id, region, label)`,
];

// Without a default the uniqueness check's reading of the given failed the
// whole companion; with one, the build stored the default caller's rows.
for (const [variant, given] of [
   ["a given with no default", ""],
   ["a given with a default", " is 'east'"],
] as const)
   describe(`R4: an inline= join whose rows read a given answers live; the rest of the model serves (${variant})`, () => {
      const model = `##! experimental.givens
given: REGION :: string${given}

source: regions is duckdb.table('rg') extend {
  primary_key: id
  where: region = $REGION
}

#(materialize) storage="lake" name="wide_t" inline=["r"]
source: wide is duckdb.table('ev') extend {
  join_one: r is regions on rid = r.id
  measure: total is amt.sum()
}

#(materialize) storage="lake" name="plain_t"
source: plain is duckdb.table('ev') extend {
  measure: n is count(), total is amt.sum()
}

#(materialize) storage="lake" name="by_region_t"
source: by_region is plain extend {
  where: region = $REGION
}
`;
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model },
            seedSql,
            materialize: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("builds every source but the refused one", () => {
         expect(Object.keys(fixture.tableIds).sort()).toEqual([
            "by_region",
            "plain",
         ]);
      });

      it("answers the refused source live, equal to live, per caller", async () => {
         const rows = [];
         for (const REGION of ["east", "west"]) {
            const run = await fixture.run({
               name: `wide as ${REGION}`,
               malloy:
                  "run: wide -> { group_by: r.label; aggregate: total; order_by: label }",
               givens: { REGION },
               expectLive: true,
            });
            expect(run.servedFrom).toBeNull();
            expect(run.servedVia).not.toBe("served_copy");
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            rows.push(run.live);
         }
         // The callers see different joined rows: no one stored table holds both.
         expect(rows[0]).not.toEqual(rows[1]);
      });

      it("serves the model's other sources from storage, equal to live", async () => {
         for (const REGION of ["east", "west"]) {
            for (const malloy of [
               "run: plain -> { group_by: region; aggregate: n, total }",
               "run: by_region -> { group_by: region; aggregate: n, total }",
            ]) {
               const run = await fixture.run({
                  name: `${malloy} as ${REGION}`,
                  malloy,
                  givens: { REGION },
               });
               expect(run.servedVia).toBe("served_copy");
               expect(run.servedFrom).toBe("storage");
               expect(run.live.length).toBeGreaterThan(0);
               expect(rowDifference(run.live, run.served)).toBeUndefined();
            }
         }
      });
   });

describe("R4: one extension's chained build falls back to the raw chain alone", () => {
   // `b` reads `r`, declared on `base` above `a`, which `a`'s stored table
   // does not carry: over `a`'s build source it does not compile, so it builds
   // over the raw chain. `c` builds over `a`'s stored table.
   const model = `source: regions is duckdb.table('rg') extend { primary_key: id }

source: base is duckdb.table('ev') extend {
  join_one: r is regions on rid = r.id
}

#(materialize) storage="lake" name="a_t"
source: a is base extend {
  measure: total is amt.sum()
}

#(materialize) storage="lake" name="b_t"
source: b is a extend {
  where: r.label != 'Mid'
  dimension: big is amt > 10
}

#(materialize) storage="lake" name="c_t"
source: c is a extend {
  where: region = 'east'
  dimension: dbl is amt * 2
}
`;
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("builds the failing extension over the raw chain and its sibling over the parent's table", () => {
      expect(fixture.buildReads).toEqual({ a: [], b: [], c: ["a"] });
      // Only `c` ran in the destination over `a`'s stored table.
      expect(Object.keys(fixture.storedBuildSQL)).toEqual(["c"]);
      expect(fixture.storedBuildSQL.c).toContain(fixture.physicalTables.a);
      expect(fixture.storedBuildSQL.c).not.toMatch(/\bev\b/);
      // `b` read the warehouse: the raw chain.
      expect(fixture.builtSQL.b).toMatch(/\bev\b/);
   });

   it("serves the chained sibling from its own table, equal to live", async () => {
      const run = await fixture.run({
         name: "c",
         malloy: "run: c -> { group_by: dbl; aggregate: total }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(run.servedFrom).toBe("storage");
      expect(run.live.length).toBeGreaterThan(0);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   // `a` and `b` read `r` at query time through a join their tables do not
   // carry (`fields_not_carried`, unchanged here), so they answer live.
   for (const [name, malloy] of [
      [
         "the fallen-back extension",
         "run: b -> { group_by: big, region; aggregate: total }",
      ],
      ["the parent", "run: a -> { group_by: region; aggregate: total }"],
   ] as const) {
      it(`${name} answers equal to live`, async () => {
         const run = await fixture.run({ name, malloy, expectLive: true });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("stored the raw chain's rows for the fallen-back extension, and the parent's rows under its sibling", async () => {
      for (const name of ["b", "c"]) {
         const live = await fixture.run({
            name: `${name} rows`,
            malloy: `run: ${name} -> { group_by: id, amt; order_by: id }`,
            expectLive: true,
         });
         const stored = await fixture.storedSQL(
            (t) =>
               `SELECT id, CAST(amt AS DOUBLE) AS amt FROM ${t(name)} ORDER BY id`,
         );
         expect(live.live.length).toBeGreaterThan(0);
         expect(rowDifference(live.live, stored)).toBeUndefined();
      }
   });
});

describe("R4: a chained build keeps its parent's table past many siblings that do not compile over it", () => {
   // Seven plain sources each read `r`, which `a`'s stored table does not
   // carry: every one fails `c`'s build copy over `a`'s build source. They are
   // left out of the copy, empty, and `c` still builds over `a`'s table (one
   // round per sibling used to run out and cost the model every build).
   const siblings = Array.from(
      { length: 7 },
      (_, i) =>
         `source: x${i} is a extend {\n  dimension: l${i} is r.label\n}\n`,
   ).join("\n");
   const model = `source: regions is duckdb.table('rg') extend { primary_key: id }

source: base is duckdb.table('ev') extend {
  join_one: r is regions on rid = r.id
}

#(materialize) storage="lake" name="a_t"
source: a is base extend {
  measure: total is amt.sum()
}

${siblings}
#(materialize) storage="lake" name="c_t"
source: c is a extend {
  where: region = 'east'
  dimension: dbl is amt * 2
}
`;
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("builds the extension over its parent's stored table", () => {
      expect(fixture.buildReads).toEqual({ a: [], c: ["a"] });
      expect(fixture.storedBuildSQL.c).toContain(fixture.physicalTables.a);
      expect(fixture.storedBuildSQL.c).not.toMatch(/\bev\b/);
   });

   // The served copy of this file does not compile today: it leaves `r` out
   // of `a` (the destination cannot join it), and the plain siblings read it.
   // That is the serve side's, unchanged here, so `c` answers live, equal to
   // live; its stored rows are checked against live's below.
   it("answers the extension equal to live", async () => {
      const run = await fixture.run({
         name: "c",
         malloy: "run: c -> { group_by: dbl; aggregate: total }",
      });
      expect(run.live.length).toBeGreaterThan(0);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("stored the parent's rows under the extension, equal to live", async () => {
      const live = await fixture.run({
         name: "c rows",
         malloy: "run: c -> { group_by: id, amt; order_by: id }",
         expectLive: true,
      });
      const stored = await fixture.storedSQL(
         (t) =>
            `SELECT id, CAST(amt AS DOUBLE) AS amt FROM ${t("c")} ORDER BY id`,
      );
      expect(live.live.length).toBeGreaterThan(0);
      expect(rowDifference(live.live, stored)).toBeUndefined();
   });

   it("answers a left-out sibling equal to live", async () => {
      const run = await fixture.run({
         name: "x0",
         malloy: "run: x0 -> { group_by: l0; aggregate: total; order_by: l0 }",
         expectLive: true,
      });
      expect(run.live.length).toBeGreaterThan(0);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});
