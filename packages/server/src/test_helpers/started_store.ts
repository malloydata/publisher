// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `store` with `start()` called, as the composition root leaves it: its
 * initialization runs in the background, and `finishedInitialization`
 * settles when that is done. The constructor alone does no I/O.
 */
export function startedStore<T extends { start(): Promise<void> }>(
   store: T,
): T {
   void store.start();
   return store;
}
