// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for extension tables (materialization v2, P10): an extension that
 * declares its own `#(materialize)` has a table of its own, built over its
 * parent's build source when the two are in one `storage=` destination (so
 * Malloy's planner orders the builds), and served from its own re-root, which keeps the blocks between
 * it and its parent so it inherits every measure, join and view it had. Every
 * answer equals live: over a materialized parent, through a chain of them, an
 * extension adding a `where:` (fixed, reading a given, or the current time),
 * and one that renames (a field no inherited dimension reads: Malloy itself
 * refuses an inherited computed dimension over a renamed field, live too).
 * The path report places the parent's and the
 * extension's tables on the queried source's chain, whether the served copy
 * answered or declined.
 *
 * Every scenario is `storage=`: a colocated `#(materialize)` is not built
 * (`COLOCATED_NOT_SERVED`, #1666), so no colocated chain exists to serve; the
 * refusal of a colocated chain is covered in build_file_generator.spec.ts.
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
      (1, 'east', 10.5, DATE '2024-03-15'),
      (2, 'east', 20.0, DATE '2024-03-02'),
      (3, 'west', 5.5, DATE '2024-07-20'),
      (4, 'west', 11.0, DATE '2023-12-31'),
      (5, 'east', 3.0, DATE '2024-07-01')
    ) AS t(id, region, amt, d)`,
];

/** One chain of extension tables, `storage` on each declaration. */
function chainModel(storage: string): string {
   return `##! experimental.givens
given: REGION :: string is 'east'

source: orders is duckdb.table('ev') extend {
  dimension: is_big is amt > 10
}

#(materialize) ${storage}name="stored_t"
source: stored is orders extend {
  dimension: order_month is d.month
  measure: total is amt.sum()
  view: by_region is { group_by: region; aggregate: total }
}

#(materialize) ${storage}name="east_t"
source: east is stored extend {
  where: region = 'east'
  dimension: double_amt is amt * 2
  measure: n is count()
}

#(materialize) ${storage}name="east_big_t"
source: east_big is east extend {
  where: is_big
  dimension: label is concat(region, '!')
}

source: mid is stored extend {
  measure: avg_amt is amt.avg()
}

#(materialize) ${storage}name="mid_ext_t"
source: mid_ext is mid extend {
  dimension: tag is 'x'
}

#(materialize) ${storage}name="by_given_t"
source: by_given is stored extend {
  where: region = $REGION
}

#(materialize) ${storage}name="renamed_t"
source: renamed is stored extend {
  rename: order_id is id
  measure: id_total is order_id.sum()
}

#(materialize) ${storage}name="aliased_t"
source: aliased is stored

source: east_reader is east extend {
  dimension: shout is upper(region)
}
`;
}

/** Queries every chain answers from its own table, equal to live. */
const QUERIES = [
   [
      "the parent's measure and view, on the extension",
      "run: east -> { group_by: order_month; aggregate: total, n }",
   ],
   ["the parent's view, called on the extension", "run: east -> by_region"],
   [
      "an extension of an extension",
      "run: east_big -> { group_by: label, order_month; aggregate: total, n }",
   ],
   [
      "an extension through an unmaterialized one",
      "run: mid_ext -> { group_by: tag; aggregate: avg_amt, total }",
   ],
   [
      "an extension that renames a column",
      "run: renamed -> { group_by: region; aggregate: id_total, total }",
   ],
   [
      "a renamed column as a dimension",
      "run: renamed -> { group_by: order_id, order_month; order_by: order_id }",
   ],
   [
      "a rename of a materialized source",
      "run: aliased -> { group_by: region; aggregate: total }",
   ],
   [
      "an unmaterialized extension of an extension table",
      "run: east_reader -> { group_by: shout; aggregate: total }",
   ],
] as const;

/** The tables a path report considered, as `[name, outcome, reason]`. */
function considered(
   fixture: R4Fixture,
   path:
      | {
           considered?: {
              tableId?: string;
              outcome: string;
              reason?: string;
           }[];
        }
      | undefined,
) {
   const names = new Map(
      Object.entries(fixture.tableIds).map(([n, id]) => [id, n]),
   );
   return (path?.considered ?? []).map((c) =>
      [names.get(c.tableId ?? "") ?? c.tableId, c.outcome, c.reason].filter(
         (x) => x !== undefined,
      ),
   );
}

describe("R4: extension tables answer equal to live", () => {
   let restore: () => void;
   let storage: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      storage = await buildScenario({
         files: { "m.malloy": chainModel('storage="lake" ') },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("builds a storage= extension over its parent's stored table, each after its parent", () => {
      expect(storage.buildReads).toEqual({
         stored: [],
         east: ["stored"],
         east_big: ["east"],
         mid_ext: ["stored"],
         by_given: ["stored"],
         renamed: ["stored"],
         aliased: ["stored"],
      });
      expect(Object.keys(storage.tableIds).sort()).toEqual(
         Object.keys(storage.buildReads).sort(),
      );
      // Each extension's build ran in the destination over its parent's
      // table: the parent's table is in its SQL, the warehouse's is not.
      const parents = Object.fromEntries(
         Object.entries(storage.buildReads).filter(([, r]) => r.length > 0),
      );
      expect(Object.keys(storage.storedBuildSQL).sort()).toEqual(
         Object.keys(parents).sort(),
      );
      for (const [name, [parent]] of Object.entries(parents)) {
         const sql = storage.storedBuildSQL[name];
         expect(sql).toContain(storage.physicalTables[parent]);
         expect(sql).not.toMatch(/\bev\b/);
      }
   });

   for (const [label, fixture] of [["storage=", () => storage]] as const) {
      describe(label, () => {
         it("leaves no served-copy problem", async () => {
            expect(await fixture().servedCopyProblems()).toEqual([]);
         });

         for (const [name, malloy] of QUERIES) {
            it(name, async () => {
               const run = await fixture().run({ name, malloy });
               expect(run.servedVia).toBe("served_copy");
               expect(run.servedFrom).toBe("storage");
               expect(run.live.length).toBeGreaterThan(0);
               expect(rowDifference(run.live, run.served)).toBeUndefined();
            });
         }

         it("applies an extension's given-reading where: per caller", async () => {
            const rows = [];
            for (const REGION of ["east", "west"]) {
               const run = await fixture().run({
                  name: `by_given as ${REGION}`,
                  malloy:
                     "run: by_given -> { group_by: region; aggregate: total }",
                  givens: { REGION },
               });
               expect(run.servedVia).toBe("served_copy");
               expect(rowDifference(run.live, run.served)).toBeUndefined();
               rows.push(run.live);
            }
            expect(rows[0]).not.toEqual(rows[1]);
         });

         it("answers each extension from its own table", async () => {
            const f = fixture();
            f.setBindings(f.bindings);
            for (const name of ["east", "east_big", "mid_ext", "renamed"]) {
               const { path } = await f.runAsIs(
                  `run: ${name} -> { group_by: region }`,
               );
               expect(path?.servedBy).toMatchObject({
                  kind: "materialized",
                  tableId: f.tableIds[name],
                  via: "served_copy",
               });
            }
         });

         it("places the parent's and the extension's tables on the chain, answered or declined", async () => {
            const f = fixture();
            f.setBindings(f.bindings);
            const answered = await f.runAsIs(
               "run: east_big -> { group_by: order_month }",
            );
            expect(considered(f, answered.path)).toEqual([
               ["east_big", "used"],
            ]);
            // A timezone over a stored computed dimension declines every
            // table on the chain, nearest first, each for that reason.
            const declined = await f.runAsIs(
               "run: east_big -> { timezone: 'UTC'; group_by: order_month }",
            );
            expect(declined.path?.servedBy.kind).toBe("live");
            expect(considered(f, declined.path)).toEqual([
               ["east_big", "skipped", "TIME_ZONE"],
               ["east", "skipped", "TIME_ZONE"],
               ["stored", "skipped", "TIME_ZONE"],
            ]);
            // An unmaterialized extension reads its parent's table.
            const reader = await f.runAsIs(
               "run: east_reader -> { timezone: 'UTC'; group_by: order_month }",
            );
            expect(considered(f, reader.path)).toEqual([
               ["east", "skipped", "TIME_ZONE"],
               ["stored", "skipped", "TIME_ZONE"],
            ]);
            expect(
               JSON.stringify([answered.path, declined.path, reader.path]),
            ).not.toContain("__mat");
         });
      });
   }
});

describe("R4: an extension table adding a rolling window answers equal to live", () => {
   const B = "2026-11-10 00:00:00";
   const READS = ["2026-11-10 00:00:00", "2026-11-10 23:59:59"];
   /**
    * Three deep: a rolling window on the second, and a given-reading `where:`
    * and a rolling window of its own on the third.
    */
   const windowModel = (storage: string) => `##! experimental.givens
given: HALF :: string is 'am'

source: events is duckdb.table('events')

#(materialize) ${storage}name="ev_all"
source: ev_all is events extend {
  dimension: half_up is upper(half)
  measure: n is count()
}

#(materialize) ${storage}name="ev_recent"
source: ev_recent is ev_all extend {
  where: ts >= now - 30 days
  measure: total is amount.sum()
}

#(materialize) ${storage}name="ev_week"
source: ev_week is ev_recent extend {
  where: half = $HALF, ts >= now - 7 days
  dimension: day_of is ts.day
}
`;
   let restore: () => void;
   const fixtures: Record<string, R4Fixture> = {};
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      for (const [label, storage] of [["storage=", 'storage="lake" ']]) {
         fixtures[label] = await buildScenario({
            files: { "m.malloy": windowModel(storage) },
            seedSql: [
               `CREATE TABLE events AS
                  SELECT ts, row_number() OVER (ORDER BY ts) AS amount,
                         CASE WHEN hour(ts) < 12 THEN 'am' ELSE 'pm' END AS half
                  FROM range(TIMESTAMP '${B}' - INTERVAL 36 DAY, TIMESTAMP '${B}' + INTERVAL 2 DAY, INTERVAL 1 HOUR) t(ts)`,
            ],
            materialize: true,
            clock: B,
         });
      }
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   for (const label of ["storage="]) {
      describe(label, () => {
         it("builds each window's extension over its parent's table", () => {
            const f = fixtures[label];
            expect(f.buildReads).toEqual({
               ev_all: [],
               ev_recent: ["ev_all"],
               ev_week: ["ev_recent"],
            });
            if (label === "storage=") {
               for (const [name, parent] of [
                  ["ev_recent", "ev_all"],
                  ["ev_week", "ev_recent"],
               ]) {
                  expect(f.storedBuildSQL[name]).toContain(
                     f.physicalTables[parent],
                  );
                  expect(f.storedBuildSQL[name]).not.toMatch(/\bevents\b/);
               }
            }
         });

         for (const at of READS) {
            it(`a measure over the window, read at ${at}`, async () => {
               const f = fixtures[label];
               f.setClock(at);
               const run = await f.run({
                  name: "window",
                  malloy:
                     "run: ev_recent -> { group_by: half_up; aggregate: n, total }",
               });
               expect(run.servedVia).toBe("served_copy");
               expect(run.live.length).toBeGreaterThan(0);
               expect(rowDifference(run.live, run.served)).toBeUndefined();
            });

            for (const HALF of ["am", "pm"]) {
               it(`the third table's own window and given, as ${HALF}, read at ${at}`, async () => {
                  const f = fixtures[label];
                  f.setClock(at);
                  const run = await f.run({
                     name: `week as ${HALF}`,
                     malloy:
                        "run: ev_week -> { group_by: day_of, half_up; aggregate: n, total }",
                     givens: { HALF },
                  });
                  expect(run.servedVia).toBe("served_copy");
                  expect(run.live.length).toBeGreaterThan(0);
                  expect(rowDifference(run.live, run.served)).toBeUndefined();
               });
            }
         }
      });
   }
});
