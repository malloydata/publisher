// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";
import {
   createEventLoopYielder,
   setEventLoopYieldBudgetMsForTesting,
} from "./event_loop_yield";
import type { Model } from "./model";

describe("createEventLoopYielder", () => {
   afterEach(() => {
      setEventLoopYieldBudgetMsForTesting(10);
   });

   it("lets a timer run once the budget is spent, and not before", async () => {
      setEventLoopYieldBudgetMsForTesting(Infinity);
      let fired = false;
      setTimeout(() => (fired = true), 0);
      const neverYields = createEventLoopYielder();
      for (let i = 0; i < 50; i++) await neverYields.maybeYield();
      expect(fired).toBe(false);

      setEventLoopYieldBudgetMsForTesting(0);
      const alwaysYields = createEventLoopYielder();
      // A zero timer is due after ~1 ms, and a setImmediate turn can land
      // before the timers phase comes round, so give it a bounded while.
      const until = performance.now() + 1000;
      while (!fired && performance.now() < until) {
         await alwaysYields.maybeYield();
      }
      expect(fired).toBe(true);
   });
});

/**
 * Render-tag validation prepares every annotated view, and none of it does
 * I/O, so before the yielder the whole loop was one macrotask. A timer set
 * when validation starts must now fire before it ends, and the findings must
 * be the same ones, in the same order, as a run that never yields.
 */
describe("render-tag validation yields to the event loop", () => {
   let rootDir: string;
   const VIEWS = 120;

   function bigModel(): string {
      const views = Array.from({ length: VIEWS }, (_, i) =>
         // Every fifth view carries a finding (an inert colspan), so the
         // ordering of findings across yields is exercised too.
         i % 5 === 0
            ? `  # dashboard
  view: v${i} is {
    nest:
      # colspan=2
      kpis
    nest: trend
  }`
            : `  # bar_chart
  view: v${i} is { group_by: one; aggregate: total_two }`,
      ).join("\n");
      return `source: nums is duckdb.sql("SELECT 1 as one, 2 as two") extend {
  measure:
    total_one is one.sum()
    total_two is two.sum()
  # big_value
  view: kpis is { aggregate: total_one, total_two }
  view: trend is { group_by: one; aggregate: total_two }
${views}
}
`;
   }

   async function loadModel(): Promise<Model> {
      const envPath = path.join(rootDir, "env");
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "fixture" }),
      );
      await fs.writeFile(path.join(dir, "m.malloy"), bigModel());
      const env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
      return (await env.getPackage("pkg", false)).getModel("m.malloy")!;
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-yield-"));
   });

   afterEach(async () => {
      setEventLoopYieldBudgetMsForTesting(10);
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   /** Validates, and says whether a timer set at the start fired before the end. */
   async function validate(model: Model) {
      let firedAt: number | undefined;
      setTimeout(() => (firedAt = performance.now()), 0);
      const findings = await model.validateRenderTags();
      const doneAt = performance.now();
      return {
         findings,
         timerFiredFirst: firedAt !== undefined && firedAt <= doneAt,
      };
   }

   it(
      "fires a timer during a long validation without changing its findings",
      async () => {
         const model = await loadModel();

         setEventLoopYieldBudgetMsForTesting(Infinity);
         const blocking = await validate(model);
         // The control: with no yields the timer waits for the whole loop.
         expect(blocking.timerFiredFirst).toBe(false);

         setEventLoopYieldBudgetMsForTesting(0);
         const yielding = await validate(model);
         expect(yielding.timerFiredFirst).toBe(true);

         setEventLoopYieldBudgetMsForTesting(10);
         const defaulted = await validate(model);

         expect(blocking.findings.length).toBe(VIEWS / 5);
         expect(yielding.findings).toEqual(blocking.findings);
         expect(defaulted.findings).toEqual(blocking.findings);
      },
      { timeout: 60000 },
   );
});
