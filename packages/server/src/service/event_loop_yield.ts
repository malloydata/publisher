// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Cooperative yielding for long main-thread loops.
 *
 * A package load's main-thread work (hydrating models, render-tag validation,
 * the persist build plan) is a loop of steps that each `await`, but nothing in
 * a step does real I/O, so the whole loop ran as one macrotask: a large model's
 * validation was a single ~1 s stall in which no other request was served. A
 * loop that calls {@link EventLoopYielder.maybeYield} between steps hands the
 * event loop a turn (timers, I/O, other requests) once the budget has passed
 * since it last yielded, and otherwise continues at once. The budget is
 * wall-clock time, not time the loop held the event loop: a step that awaits
 * real I/O already gives the loop a turn but does not restart the budget, so
 * the next call may yield once more than it needed to, which costs one
 * macrotask. The steps, their order and their results are unchanged; only
 * what else may run between them.
 */

/** Wall-clock time since a loop last yielded before it yields again, in ms. */
const DEFAULT_BUDGET_MS = 10;
let budgetMs = DEFAULT_BUDGET_MS;

/** One macrotask turn: lets timers and I/O callbacks queued meanwhile run. */
export function yieldToEventLoop(): Promise<void> {
   return new Promise((resolve) => setImmediate(resolve));
}

export interface EventLoopYielder {
   /** Yields if the loop has run for the budget since it last yielded. */
   maybeYield(): Promise<void>;
}

/** A yielder for one loop; its clock starts now. */
export function createEventLoopYielder(): EventLoopYielder {
   let since = performance.now();
   return {
      async maybeYield() {
         if (performance.now() - since < budgetMs) return;
         await yieldToEventLoop();
         since = performance.now();
      },
   };
}

/**
 * Visible for tests: `0` yields at every step, `Infinity` never. Returns the
 * previous budget. Do NOT call from production code.
 */
export function setEventLoopYieldBudgetMsForTesting(ms: number): number {
   const previous = budgetMs;
   budgetMs = ms;
   return previous;
}
