// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { AsyncLocalStorage } from "async_hooks";

/**
 * Work that serves no one, such as a what-if over a proposed package, runs
 * inside this scope. Within it, logs are dropped unless debug is on (see logger.ts)
 * and lazily created metric instruments record nothing (see telemetry.ts), so a
 * hypothetical load, bind or route never reads to an operator as something
 * that happened. Scoped by async context: concurrent requests outside it are
 * unaffected.
 */
const scope = new AsyncLocalStorage<true>();

/** Run `fn` as hypothetical work (see the module comment). */
export function runHypothetical<T>(fn: () => Promise<T>): Promise<T> {
   return scope.run(true, fn);
}

/** Whether the caller is inside {@link runHypothetical}. */
export function isHypothetical(): boolean {
   return scope.getStore() === true;
}
