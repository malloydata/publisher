// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Characterization of {@link PlanLifecycle}: one package version's build plan
 * as it is loaded, marked stale by a connection edit, re-derived and waited
 * on. Pinned through its public surface with a fake owner and a `derive`
 * the spec controls.
 */

import { describe, expect, it } from "bun:test";
import { BuildPlanRefreshingError } from "../../errors";
import type { LoadPackageOutcome } from "../../package_load/package_load_pool";
import {
   failedWorkerPlan,
   makePlanOutcome,
   makeTwoConnectionPlan,
   okWorkerPlan,
} from "../../test_helpers/materialization_test_fixtures";
import { PlanLifecycle } from "./plan_lifecycle";

type WorkerPlan = LoadPackageOutcome["buildPlan"];

/** A lifecycle over a package with no models, counting its refresh callbacks. */
function lifecycle(): { plan: PlanLifecycle; refreshed: () => number } {
   let refreshed = 0;
   const plan = new PlanLifecycle(
      {
         packageName: () => "pkg",
         versionId: () => undefined,
         metadata: () => ({}) as never,
         buildPlanPackage: {} as never,
      },
      () => refreshed++,
   );
   return { plan, refreshed: () => refreshed };
}

/** A promise the spec settles, for a `derive` that is still running. */
function deferred<T>(): {
   promise: Promise<T>;
   resolve: (value: T) => void;
   reject: (err: unknown) => void;
} {
   let resolve!: (value: T) => void;
   let reject!: (err: unknown) => void;
   const promise = new Promise<T>((res, rej) => {
      resolve = res;
      reject = rej;
   });
   return { promise, resolve, reject };
}

/** Whether `p` has settled, after the microtasks queued so far ran. */
async function settled(p: Promise<unknown>): Promise<boolean> {
   let done = false;
   void p.then(
      () => (done = true),
      () => (done = true),
   );
   for (let i = 0; i < 20; i++) await Promise.resolve();
   return done;
}

/** Everything a good plan over two connections carries, each side table set. */
function richOutcome() {
   return makePlanOutcome({
      plan: makeTwoConnectionPlan(),
      sourceEligibility: {
         eligible: ["a@m.malloy", "b@m.malloy"],
         refused: {},
         gateKept: ["a@m.malloy"],
      },
      databaseSourceEligibility: {
         eligibleEntityIds: new Set(["addr-a", "addr-b"]),
         refused: {},
      },
      buildConnectionNames: ["conn_skipped"],
      materializePlans: { "a@m.malloy": { sourceID: "a@m.malloy" } as never },
      rollupsKeepingGates: ["r@m.malloy"],
      refusedRollups: {
         "q@m.malloy": { baseSourceName: "base", grain: ["g"] } as never,
      },
      refusalNotices: { "q@m.malloy": { code: "rollup-not-built" } },
      compilerKeys: { "addr-a": "build-a" },
      annotationFindings: [{ code: "finding", message: "m" } as never],
   });
}

describe("PlanLifecycle.load", () => {
   it("adopts the worker's plan and everything compiled beside it", async () => {
      const { plan } = lifecycle();
      const outcome = richOutcome();
      expect(await plan.load(okWorkerPlan(outcome))).toBeNumber();
      expect(plan.failed).toBe(false);
      expect(plan.buildPlan).toBe(outcome.plan);
      expect(plan.servingPlan()).toBe(outcome.plan);
      expect(plan.sourceEligibility).toBe(outcome.sourceEligibility);
      expect(plan.databaseSourceEligibility).toBe(
         outcome.databaseSourceEligibility,
      );
      expect([...plan.rollupsKeepingGates]).toEqual(["r@m.malloy"]);
      expect(plan.compilerKeys).toEqual({ "addr-a": "build-a" });
      // Every connection a source builds on, the skipped ones included.
      expect([...(plan.connectionNames() ?? [])].sort()).toEqual([
         "conn_a",
         "conn_b",
         "conn_skipped",
      ]);
   });

   it("fails closed on a failed plan: no plan, eligibility unknown, any connection edit re-derives", async () => {
      const { plan } = lifecycle();
      await plan.load(okWorkerPlan(richOutcome()));
      expect(await plan.load(failedWorkerPlan())).toBeUndefined();
      expect(plan.failed).toBe(true);
      expect(plan.buildPlan).toBeNull();
      expect(plan.servingPlan()).toBeNull();
      expect(plan.sourceEligibility).toBeUndefined();
      expect(plan.databaseSourceEligibility).toBeUndefined();
      expect(plan.incrementalPolicySources).toEqual([]);
      expect([...plan.rollupsKeepingGates]).toEqual([]);
      expect(plan.refusedRollups).toEqual({});
      expect(plan.refusalNotices).toEqual({});
      expect(plan.compilerKeys).toEqual({});
      expect(plan.connectionNames()).toBeNull();
   });

   it("a failed load keeps nothing of the previous load, its findings and materialize plans included", async () => {
      const { plan } = lifecycle();
      await plan.load(okWorkerPlan(richOutcome()));
      await plan.load(failedWorkerPlan());
      expect(plan.annotationFindings).toEqual([]);
      expect(plan.materializePlans).toEqual({});
   });
});

describe("PlanLifecycle staleness and refresh", () => {
   it("is current until a connection edit, and a refresh of a current plan derives nothing", async () => {
      const { plan, refreshed } = lifecycle();
      await plan.load(okWorkerPlan());
      expect(plan.isStale()).toBe(false);
      await plan.whenCurrent(1);
      let derives = 0;
      expect(
         await plan.refresh(async () => {
            derives++;
            return okWorkerPlan();
         }),
      ).toBe(true);
      expect(derives).toBe(0);
      // A refresh with nothing to do does not call back.
      expect(refreshed()).toBe(0);
   });

   it("re-derives a burst of edits once, and releases the waiters", async () => {
      const { plan, refreshed } = lifecycle();
      await plan.load(okWorkerPlan());
      plan.markStale(["conn_a"]);
      plan.markStale(["conn_b"]);
      plan.markStale();
      const waiting = plan.whenCurrent(60_000);
      expect(await settled(waiting)).toBe(false);
      let derives = 0;
      const derive = async () => {
         derives++;
         return okWorkerPlan();
      };
      const [first, second] = await Promise.all([
         plan.refresh(derive),
         plan.refresh(derive),
      ]);
      expect([first, second]).toEqual([true, true]);
      expect(derives).toBe(1);
      expect(refreshed()).toBe(1);
      expect(plan.isStale()).toBe(false);
      await waiting;
   });

   it("runs refreshes one at a time, in the order they were asked", async () => {
      const { plan } = lifecycle();
      await plan.load(okWorkerPlan());
      plan.markStale();
      const events: string[] = [];
      const gate = deferred<WorkerPlan>();
      const first = plan.refresh(async () => {
         events.push("first started");
         const result = await gate.promise;
         events.push("first finished");
         return result;
      });
      const second = plan.refresh(
         async () => {
            events.push("second started");
            return okWorkerPlan();
         },
         { retryFailed: true },
      );
      await settled(second);
      expect(events).toEqual(["first started"]);
      gate.resolve(okWorkerPlan());
      await Promise.all([first, second]);
      // The second found the plan current and derived nothing.
      expect(events).toEqual(["first started", "first finished"]);
   });

   it("releases the waiters only by a refresh that started after the latest edit", async () => {
      const { plan } = lifecycle();
      await plan.load(okWorkerPlan());
      plan.markStale(["conn_a"]);
      const waiting = plan.whenCurrent(60_000);
      const gate = deferred<WorkerPlan>();
      const first = plan.refresh(() => gate.promise);
      await settled(first);
      // An edit while the first re-derivation runs: its result predates it.
      plan.markStale(["conn_a"]);
      gate.resolve(okWorkerPlan());
      expect(await first).toBe(true);
      expect(plan.isStale()).toBe(true);
      expect(await settled(waiting)).toBe(false);
      expect(await plan.refresh(async () => okWorkerPlan())).toBe(true);
      expect(plan.isStale()).toBe(false);
      await waiting;
   });

   it("answers a reader past the timeout with BuildPlanRefreshingError (a 503)", async () => {
      const { plan } = lifecycle();
      await plan.load(okWorkerPlan());
      plan.markStale();
      const err = await plan.whenCurrent(5).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(BuildPlanRefreshingError);
      expect((err as Error).message).toContain("package pkg");
      // The plan is still stale: only a refresh clears it.
      expect(plan.isStale()).toBe(true);
   });

   it("fails a re-derivation for a package that serves an older model than its files", async () => {
      const { plan, refreshed } = lifecycle();
      await plan.load(okWorkerPlan());
      plan.markStale();
      expect(
         await plan.refresh(async () => okWorkerPlan(), {
            isStale: () => true,
         }),
      ).toBe(false);
      expect(plan.failed).toBe(true);
      // A compute failure, not the pool's.
      expect(plan.lastRefreshFailedTransiently()).toBe(false);
      expect(plan.isStale()).toBe(false);
      expect(refreshed()).toBe(1);
   });

   it("tells a pool failure (transient) from a compute failure", async () => {
      const { plan } = lifecycle();
      await plan.load(okWorkerPlan());
      plan.markStale();
      expect(
         await plan.refresh(async () => {
            throw new Error("worker timed out");
         }),
      ).toBe(false);
      expect(plan.lastRefreshFailedTransiently()).toBe(true);
      plan.markStale();
      expect(await plan.refresh(async () => failedWorkerPlan())).toBe(false);
      expect(plan.lastRefreshFailedTransiently()).toBe(false);
   });

   it("re-derives a failed plan only when a retry is asked for", async () => {
      const { plan } = lifecycle();
      await plan.load(okWorkerPlan());
      plan.markStale();
      await plan.refresh(async () => failedWorkerPlan());
      let derives = 0;
      const derive = async () => {
         derives++;
         return okWorkerPlan();
      };
      // Not stale, so a plain refresh does nothing and reports the failure.
      expect(await plan.refresh(derive)).toBe(false);
      expect(derives).toBe(0);
      expect(await plan.refresh(derive, { retryFailed: true })).toBe(true);
      expect(derives).toBe(1);
      expect(plan.failed).toBe(false);
   });
});

describe("PlanLifecycle after a failed refresh", () => {
   /** A good plan loaded, an edit to `conn_a`, then a refresh that fails. */
   async function failedAfterEdit() {
      const { plan } = lifecycle();
      const outcome = richOutcome();
      await plan.load(okWorkerPlan(outcome));
      plan.markStale(["conn_a"]);
      const derived = await plan.refresh(async () => failedWorkerPlan());
      return { plan, outcome, derived };
   }

   it("keeps serving from the last good plan, refusing only the edited connection's database tables", async () => {
      const { plan, outcome, derived } = await failedAfterEdit();
      expect(derived).toBe(false);
      expect(plan.failed).toBe(true);
      // No route or run reads the last good plan as the plan...
      expect(plan.buildPlan).toBeNull();
      // ...but bindings still match against it.
      expect(plan.servingPlan()).toBe(outcome.plan);
      expect(plan.sourceEligibility).toBe(outcome.sourceEligibility);
      expect(plan.compilerKeys).toEqual({ "addr-a": "build-a" });
      expect([...plan.databaseSourceEligibility!.eligibleEntityIds]).toEqual([
         "addr-b",
      ]);
      expect(plan.databaseSourceEligibility!.refused).toEqual({
         "addr-a":
            "its connection changed and the build plan could not be re-derived yet",
      });
      // Unknown while failed: any connection edit re-derives.
      expect(plan.connectionNames()).toBeNull();
   });

   it("a refresh that fails after a good plan keeps the whole last good record, every side table included, behind a null buildPlan", async () => {
      const { plan, outcome } = await failedAfterEdit();
      expect(plan.buildPlan).toBeNull();
      expect([...plan.rollupsKeepingGates]).toEqual(["r@m.malloy"]);
      expect(plan.refusedRollups).toBe(outcome.refusedRollups);
      expect(plan.refusalNotices).toBe(outcome.refusalNotices);
      expect(plan.annotationFindings).toBe(outcome.annotationFindings);
      expect(plan.materializePlans).toBe(outcome.materializePlans);
      expect(plan.incrementalPolicySources).toEqual([]);
   });

   it("keeps the last good record across a second failed refresh, less both edited connections", async () => {
      const { plan, outcome } = await failedAfterEdit();
      plan.markStale(["conn_b"]);
      expect(await plan.refresh(async () => failedWorkerPlan())).toBe(false);
      expect(plan.servingPlan()).toBe(outcome.plan);
      expect([...plan.databaseSourceEligibility!.eligibleEntityIds]).toEqual(
         [],
      );
      expect(
         Object.keys(plan.databaseSourceEligibility!.refused).sort(),
      ).toEqual(["addr-a", "addr-b"]);
   });

   it("forgets the edited connections once a later refresh derives", async () => {
      const { plan } = await failedAfterEdit();
      expect(
         await plan.refresh(async () => okWorkerPlan(richOutcome()), {
            retryFailed: true,
         }),
      ).toBe(true);
      // An edit to the other connection, and a failure: only it is refused now.
      plan.markStale(["conn_b"]);
      await plan.refresh(async () => failedWorkerPlan());
      expect([...plan.databaseSourceEligibility!.eligibleEntityIds]).toEqual([
         "addr-a",
      ]);
   });

   it("restores nothing when there was no good plan to restore", async () => {
      const { plan } = lifecycle();
      await plan.load(failedWorkerPlan());
      plan.markStale(["conn_a"]);
      await plan.refresh(async () => failedWorkerPlan());
      expect(plan.servingPlan()).toBeNull();
      expect(plan.sourceEligibility).toBeUndefined();
      expect(plan.databaseSourceEligibility).toBeUndefined();
   });
});

describe("PlanLifecycle.withdraw", () => {
   const derivedAgain = async (plan: PlanLifecycle) => {
      plan.markStale();
      expect(await plan.refresh(async () => okWorkerPlan(richOutcome()))).toBe(
         true,
      );
   };

   it("a derivation replaces the withdrawn set when nothing holds it", async () => {
      const { plan } = lifecycle();
      await plan.load(okWorkerPlan(richOutcome()));
      plan.withdraw(["a@m.malloy"]);
      expect([...plan.withdrawn]).toEqual(["addr-a"]);
      await derivedAgain(plan);
      expect([...plan.withdrawn]).toEqual([]);
   });

   it("carries a held withdrawn set into every later derivation, until released", async () => {
      const { plan } = lifecycle();
      await plan.load(okWorkerPlan(richOutcome()));
      plan.withdraw(["a@m.malloy"]);
      plan.holdWithdrawn(true);
      await derivedAgain(plan);
      expect([...plan.withdrawn]).toEqual(["addr-a"]);
      await derivedAgain(plan);
      expect([...plan.withdrawn]).toEqual(["addr-a"]);
      plan.holdWithdrawn(false);
      await derivedAgain(plan);
      expect([...plan.withdrawn]).toEqual([]);
   });
});
