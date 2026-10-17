// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * True once `predicate` is, or false if `timeoutMs` passes first. For waiting
 * on a condition the test cannot await directly (a gate a background job is
 * parked at, a file a child process writes); when the work is a promise or a
 * queue with an idle hook, await that instead.
 */
export async function pollUntil(
   predicate: () => Promise<boolean> | boolean,
   timeoutMs = 2_000,
   intervalMs = 5,
): Promise<boolean> {
   const deadline = Date.now() + timeoutMs;
   for (;;) {
      if (await predicate()) return true;
      if (Date.now() >= deadline) return false;
      await new Promise((r) => setTimeout(r, intervalMs));
   }
}

/**
 * Call `produce` until `done` accepts its value and return that value; throw
 * with the last value if `timeoutMs` passes first. For "ask again until the
 * background sync lands" loops.
 */
export async function pollFor<T>(
   produce: () => Promise<T>,
   done: (value: T) => boolean,
   timeoutMs = 2_000,
   intervalMs = 5,
): Promise<T> {
   let last!: T;
   const reached = await pollUntil(
      async () => done((last = await produce())),
      timeoutMs,
      intervalMs,
   );
   if (!reached) {
      throw new Error(
         `condition not reached within ${timeoutMs}ms; last value: ${JSON.stringify(last)?.slice(0, 500)}`,
      );
   }
   return last;
}

/** A semantic-search result that is not the cold-start "indexing" answer. */
export const notIndexing = (result: object): boolean =>
   !("unavailable" in result) ||
   (result as { unavailable: unknown }).unavailable !== "indexing";
