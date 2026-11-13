// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a rolling window over a hidden dimension, and for containment of a
 * served-copy compile failure (materialization v2, phase 3).
 *
 * A `#(materialize)` source whose rolling-window `where:` reads an `internal`
 * or `private` dimension it defines (`internal dimension: t2 is ts; where:
 * t2 >= now - 60 days`) keeps that dimension's definition in the served
 * copy, computed at read (P3). The window was written on the stored table,
 * ahead of the definition, so the model's whole served copy failed with
 * "'t2' is not defined" and every source in it answered live. The re-root now
 * leaves such a term to the kept text, which applies it as the author wrote
 * it (and the serve side confirms it in the served source's compiled
 * filters). Lower, upper and two-sided windows, own, inherited from a base
 * that hides the dimension, and kept by an extension table, are each read at
 * pinned clocks and equal to live, and the hidden dimension stays exactly as
 * hidden as live keeps it.
 *
 * Containment: a re-root that does not compile (here, a stand-in for a
 * re-root bug: a binding whose stored type no longer fits the source's own
 * measure) costs that source alone (`reroot_not_compiled`, answered live);
 * the model's other sources keep their tables.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
} from "./harness";

const B = "2026-11-10 00:00:00"; // build start
const READS = [
   "2026-11-10 00:00:00",
   "2026-11-10 12:00:00",
   "2026-11-10 23:59:59",
   "2026-11-30 00:00:00",
];

const seedSql = [
   `CREATE TABLE events AS
      SELECT ts, row_number() OVER (ORDER BY ts) AS amount,
             CASE WHEN hour(ts) % 2 = 0 THEN 'acme' ELSE 'globex' END AS org
      FROM range(TIMESTAMP '${B}' - INTERVAL 120 DAY, TIMESTAMP '${B}' + INTERVAL 2 DAY, INTERVAL 1 HOUR) t(ts)`,
];

const model = `##! experimental { access_modifiers }

source: events is duckdb.table('events')

#(materialize) storage="lake"
source: w_internal is events extend {
  internal dimension: t2 is ts
  where: t2 >= now - 60 days
  measure: n is count(), total is amount.sum()
}

#(materialize) storage="lake"
source: w_private is events extend {
  private dimension: t2 is ts
  where: t2 >= now - 60 days
  measure: n is count(), total is amount.sum()
}

#(materialize) storage="lake"
source: w_upper is events extend {
  internal dimension: t2 is ts
  where: t2 < now - 30 days
  measure: n is count(), total is amount.sum()
}

#(materialize) storage="lake"
source: w_between is events extend {
  private dimension: t2 is ts
  where: t2 >= now - 60 days and t2 < now - 30 days
  measure: n is count(), total is amount.sum()
}

source: hidden_base is events extend {
  internal dimension: t2 is ts
  where: t2 >= now - 60 days
}

#(materialize) storage="lake"
source: w_inherited is hidden_base extend {
  measure: n is count(), total is amount.sum()
}

#(materialize) storage="lake"
source: w_extension is w_internal extend {
  measure: top is amount.max()
}

#(materialize) storage="lake"
source: plain is events extend {
  measure: n is count(), total is amount.sum()
}

#(materialize) storage="lake"
source: broken is events extend {
  measure: n is count(), total is amount.sum()
}

source: joiner is events extend {
  join_one: w is w_private on ts = w.ts
}
`;

const WINDOWED = [
   "w_internal",
   "w_private",
   "w_upper",
   "w_between",
   "w_inherited",
   "w_extension",
];

/** The error a run throws, or undefined when it answers. */
async function failure(run: () => Promise<unknown>): Promise<unknown> {
   try {
      await run();
      return undefined;
   } catch (error) {
      return error;
   }
}

/**
 * Live and the served copy alike: both refuse, the served copy's own
 * compile included, or both answer with the same rows, served from storage.
 */
async function expectParity(
   fixture: R4Fixture,
   malloy: string,
   answers: boolean,
): Promise<void> {
   fixture.setBindings([]);
   let live: Row[] | undefined;
   const liveError = await failure(async () => {
      live = (await fixture.runAsIs(malloy)).rows;
   });
   fixture.setBindings(fixture.bindings);
   let served: { rows: Row[]; servedFrom: string | null } | undefined;
   const servedError = await failure(async () => {
      served = await fixture.runAsIs(malloy);
   });
   if (!answers) {
      expect(liveError).toBeDefined();
      expect(servedError).toBeDefined();
      const copyError = await failure(async () => {
         const { runnable, virtualMap } = await fixture
            .servedCopy()
            .loadQuery(malloy, {});
         await runnable.getSQL({ virtualMap } as never);
      });
      expect(copyError).toBeDefined();
      return;
   }
   expect(liveError).toBeUndefined();
   expect(servedError).toBeUndefined();
   expect(served?.servedFrom).toBe("storage");
   expect(rowDifference(live ?? [], served?.rows ?? [])).toBeUndefined();
}

describe("R4: a rolling window over a hidden dimension, served at a pinned clock", () => {
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
         clock: B,
      });
      // A stand-in for a re-root bug: `broken`'s stored `amount` read back
      // as a string, which its own `amount.sum()` cannot compile over.
      // In place: the fixture's runs read the same binding objects.
      const broken = fixture.bindings.filter((b) =>
         b.sourceName?.startsWith("broken"),
      );
      expect(broken.length).toBe(1);
      (broken[0] as { schema: (typeof broken)[0]["schema"] }).schema =
         broken[0].schema.map((c) =>
            c.name === "amount" ? { ...c, type: "VARCHAR" } : c,
         );
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("compiles the served copy with every source but the broken one re-rooted", async () => {
      const problems = await fixture.servedCopyProblems();
      // `joiner` joins `w_private` from the origin connection, which a
      // stored `w_private` takes across connections: left out, as always.
      expect(problems.map((p) => [p.sourceName, p.reason])).toEqual([
         ["broken", "reroot_not_compiled"],
         ["joiner", "cross_connection_join"],
      ]);
   });

   it("answers the source whose re-root does not compile live, equal to live", async () => {
      const run = await fixture.run({
         name: "broken",
         malloy: "run: broken -> { group_by: org; aggregate: n, total }",
         expectLive: true,
      });
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("keeps every other source's table", async () => {
      fixture.setClock(READS[0]);
      const run = await fixture.run({
         name: "plain",
         malloy: "run: plain -> { group_by: org; aggregate: n, total }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(run.servedFrom).toBe("storage");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   for (const at of READS) {
      for (const source of WINDOWED) {
         for (const [shape, malloy] of [
            ["a measure", `run: ${source} -> { aggregate: n, total }`],
            [
               "grouped by a stored dimension",
               `run: ${source} -> { group_by: org; aggregate: n, total }`,
            ],
         ] as const) {
            it(`${source}: ${shape}, read at ${at}`, async () => {
               fixture.setClock(at);
               const run = await fixture.run({ name: source, malloy });
               expect(run.servedVia).toBe("served_copy");
               expect(run.servedFrom).toBe("storage");
               expect(run.live.length).toBeGreaterThan(0);
               expect(rowDifference(run.live, run.served)).toBeUndefined();
            });
         }
      }
   }

   it("moves with the clock: weeks later a lower bound counts fewer rows, as live", async () => {
      fixture.setClock(READS[0]);
      const atB = await fixture.run({
         name: "at b",
         malloy: "run: w_private -> { aggregate: n }",
      });
      fixture.setClock(READS[1]);
      const later = await fixture.run({
         name: "mid",
         malloy: "run: w_private -> { aggregate: n }",
      });
      expect(rowDifference(later.live, later.served)).toBeUndefined();
      fixture.setClock(READS[3]);
      const weeks = await fixture.run({
         name: "weeks",
         malloy: "run: w_private -> { aggregate: n }",
      });
      expect(Number(weeks.served[0].n)).toBeLessThan(Number(atB.served[0].n));
      expect(rowDifference(weeks.live, weeks.served)).toBeUndefined();
   });

   // Every caller path to the hidden dimension: refused exactly where live
   // refuses (Malloy lets a caller's own `extend` or `include` read an
   // internal field, never a private one), answered with live's rows where
   // live answers.
   const refused: [string, string][] = [];
   const answered: [string, string][] = [];
   for (const source of ["w_internal", "w_private", "w_between"]) {
      const isPrivate = source !== "w_internal";
      refused.push(
         [`${source}: select`, `run: ${source} -> { select: t2 }`],
         [`${source}: group_by`, `run: ${source} -> { group_by: t2 }`],
         [
            `${source}: a wildcard and the name`,
            `run: ${source} -> { select: *, t2 }`,
         ],
         [
            `${source}: a nest`,
            `run: ${source} -> { nest: x is { group_by: t2 } }`,
         ],
         [`${source}: index by name`, `run: ${source} -> { index: t2 }`],
         [
            `${source}: a caller rename, still hidden`,
            `run: ${source} extend { rename: r is t2 } -> { group_by: r }`,
         ],
         [
            `${source}: raw SQL interpolating it`,
            `run: ${source} -> { group_by: x is sql_string("\${t2}") }`,
         ],
         [
            `${source}: a stored name the re-root could have used`,
            `run: ${source} -> { group_by: t2__stored }`,
         ],
      );
      (isPrivate ? refused : answered).push(
         [
            `${source}: a caller extend reading it`,
            `run: ${source} extend { dimension: d is t2::string } -> { group_by: org; aggregate: n, m is max(d) }`,
         ],
         [
            `${source}: a caller include making it public`,
            `run: ${source} include { public: t2, n } -> { aggregate: n, m is max(t2) }`,
         ],
      );
      answered.push(
         [
            `${source}: select *`,
            `run: ${source} -> { select: * } -> { aggregate: c is count() }`,
         ],
         [
            `${source}: index *`,
            `run: ${source} -> { index: * } -> { aggregate: c is count() }`,
         ],
      );
   }
   refused.push(
      [
         "a private dimension through the author's join",
         "run: joiner -> { group_by: w.t2 }",
      ],
      [
         "a private dimension through a caller join",
         "run: events extend { join_one: w is w_private on ts = w.ts } -> { group_by: w.t2 }",
      ],
   );
   for (const [name, malloy] of refused) {
      it(`refuses ${name}, as live does`, async () => {
         fixture.setClock(READS[1]);
         await expectParity(fixture, malloy, false);
      });
   }
   for (const [name, malloy] of answered) {
      it(`answers ${name} with live's rows`, async () => {
         fixture.setClock(READS[1]);
         await expectParity(fixture, malloy, true);
      });
   }
});
