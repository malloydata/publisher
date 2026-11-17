// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One query, one set of bound tables (C1b). A rebind that lands while a query
 * is being routed must not split it: every leg, the run and the path report
 * read the tables bound when the query started, and the next query reads the
 * new ones.
 *
 * Each case swaps the model's bindings from inside a leg's attempt, the
 * moment between the start of the query and its run, and holds the answer to
 * the tables bound before the swap.
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import {
   installSettings,
   useMaterializationOn,
} from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE orders_raw AS SELECT * FROM (VALUES
      (1, 10.0, 'A', 'east'),
      (2, 20.0, 'A', 'west'),
      (3, 30.0, 'B', 'east'),
      (4,  5.0, 'B', 'east'),
      (5, 12.5, 'C', 'west')
    ) AS t(id, amount, category, region)`,
];

/** Replace `leg[method]` with one that runs `before` first; returns the undo. */
function interpose(
   leg: Record<string, unknown>,
   method: string,
   before: () => void,
): () => void {
   const original = leg[method] as (...args: unknown[]) => unknown;
   leg[method] = function (this: unknown, ...args: unknown[]) {
      before();
      return original.apply(this, args);
   };
   return () => {
      leg[method] = original;
   };
}

/** A private leg of the fixture's model, by its field name. */
function legOf(fixture: R4Fixture, name: string): Record<string, unknown> {
   return (fixture.model as unknown as Record<string, Record<string, unknown>>)[
      name
   ];
}

describe("C1b: a rebind between pin and run does not split a served-copy query", () => {
   let fixture: R4Fixture;
   let undo: (() => void) | undefined;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            "m.malloy": `#(materialize) storage="lake"
source: orders is duckdb.table('orders_raw') extend {
  measure: total is amount.sum()
}
`,
         },
         seedSql,
         materialize: true,
      });
   });
   afterEach(() => {
      undo?.();
      undo = undefined;
      fixture.setBindings(fixture.bindings);
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   const malloy = "run: orders -> { group_by: category; aggregate: total }";

   it("serves from the stored table with nothing swapped", async () => {
      const run = await fixture.run({ name: "unswapped", malloy });
      expect(run.servedFrom).toBe("storage");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("answers from the tables bound when the query started, though they are unbound mid-query", async () => {
      fixture.setBindings([]);
      const live = await fixture.runAsIs(malloy);
      fixture.setBindings(fixture.bindings);
      // Unbind every table once the served copy is asked, after the query
      // started and before it runs.
      undo = interpose(legOf(fixture, "servedCopy"), "loadQuery", () =>
         fixture.setBindings([]),
      );
      const served = await fixture.runAsIs(malloy);
      expect(served.servedFrom).toBe("storage");
      expect(served.path?.servedBy?.kind).not.toBe("live");
      expect(rowDifference(live.rows, served.rows)).toBeUndefined();
   });

   it("reads the new bindings on the next query", async () => {
      fixture.setBindings([]);
      const next = await fixture.runAsIs(malloy);
      expect(next.servedFrom).toBeNull();
   });
});

describe("C1b: a colocated rollup and its query read the same manifest", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   let undo: (() => void) | undefined;
   beforeAll(async () => {
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: orders is duckdb.table('orders_raw') extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}
`,
         },
         seedSql,
         preaggregate: true,
      });
   });
   afterEach(() => {
      undo?.();
      undo = undefined;
      fixture.setBindings(fixture.bindings);
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const malloy = "run: orders -> { group_by: category; aggregate: total }";

   it("serves the rollup with nothing swapped", async () => {
      const run = await fixture.run({ name: "unswapped", malloy });
      expect(run.servedBy?.kind).toBe("rollup");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("runs the rollup on the manifest the query started with, though it is unbound mid-query", async () => {
      fixture.setBindings([]);
      const live = await fixture.runAsIs(malloy);
      fixture.setBindings(fixture.bindings);
      undo = interpose(legOf(fixture, "rollupVariants"), "attempt", () =>
         fixture.setBindings([]),
      );
      const served = await fixture.runAsIs(malloy);
      // The rollup's table was named by the manifest the run read, and the
      // path report reads that manifest too.
      expect(served.path?.servedBy?.kind).toBe("rollup");
      expect(rowDifference(live.rows, served.rows)).toBeUndefined();
   });
});
