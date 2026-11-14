// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Cooperative yielding for long main-thread loops.
 *
 * A package load's main-thread steps each `await` but do no real I/O, so
 * without yielding the whole loop runs as one macrotask and a large model
 * stalls every other request for ~1 s. {@link EventLoopYielder.maybeYield}
 * hands the event loop a turn once the wall-clock budget has passed since the
 * last yield. A step that awaits real I/O does not restart the budget, so the
 * next call may yield once more than needed; that costs one macrotask.
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
