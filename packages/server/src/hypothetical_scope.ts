// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { AsyncLocalStorage } from "async_hooks";
import { withoutMetrics } from "./telemetry";

/**
 * Work that serves no one, such as a what-if over a proposed package, runs
 * inside this scope. Within it, logs are dropped unless debug is on (see logger.ts)
 * and every counter and histogram records nothing (telemetry.ts's
 * {@link withoutMetrics}, which the shadow comparison also uses), so a
 * hypothetical load, bind or route never reads to an operator as something
 * that happened. Scoped by async context: concurrent requests outside it are
 * unaffected.
 */
const scope = new AsyncLocalStorage<true>();

/** Run `fn` as hypothetical work (see the module comment). */
export function runHypothetical<T>(fn: () => Promise<T>): Promise<T> {
   return scope.run(true, () => withoutMetrics(fn));
}

/** Whether the caller is inside {@link runHypothetical}. */
export function isHypothetical(): boolean {
   return scope.getStore() === true;
}
