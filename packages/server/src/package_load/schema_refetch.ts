// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The main thread's side of a refreshing load's schema fetches (see
 * LoadPackageRequest.refreshTimestamp).
 *
 * A Malloy connection refetches a cached schema only when the request's
 * `refreshTimestamp` is later than the entry's own, and keeps the old entry
 * when the refetch fails. So a clock stepped backwards could skip a refresh,
 * and a table dropped since would keep compiling from its old columns on the
 * next load that reads the cache. A refetch therefore drops the entry first,
 * so it is fetched whatever the clocks say, and a failure leaves nothing
 * behind. Malloy's `BaseConnection` keeps that cache in a protected
 * `schemaCache` with no public way to drop an entry; a connection without one
 * (a wrapper) falls back on the timestamp, with those two limits.
 *
 * Refreshing loads that overlap share refetches: a key another load began
 * refetching after this load started is answered by that refetch, once it
 * settles, rather than fetched again.
 */
export class SchemaRefetches {
   private readonly byConnection = new WeakMap<
      object,
      Map<string, { startedAt: number; done: Promise<unknown> }>
   >();

   /**
    * `maxAgeMs`: how long a refetch is remembered, at least as long as a
    * load can run. `now` is a monotonic clock in ms, the one load start
    * times are read on.
    */
   constructor(
      private readonly maxAgeMs: number,
      private readonly now: () => number = () => performance.now(),
   ) {}

   /**
    * Fetch `keys` (`conn`'s schema-cache keys) for a refreshing load begun
    * at `loadStartedAt`: `fetch(keys, true)` refetches the keys no later
    * refetch covers, after their entries are dropped, and `fetch(keys,
    * false)` reads the rest once the refetches covering them settle.
    */
   async fetch<R>(
      conn: object,
      keys: string[],
      loadStartedAt: number,
      fetch: (keys: string[], refetch: boolean) => Promise<R>,
   ): Promise<R[]> {
      const now = this.now();
      let recent = this.byConnection.get(conn);
      if (!recent) {
         recent = new Map();
         this.byConnection.set(conn, recent);
      }
      for (const [key, refetch] of recent) {
         if (now - refetch.startedAt > this.maxAgeMs) recent.delete(key);
      }
      const covered: string[] = [];
      const waits: Promise<unknown>[] = [];
      const refetch: string[] = [];
      for (const key of new Set(keys)) {
         const earlier = recent.get(key);
         if (earlier && earlier.startedAt >= loadStartedAt) {
            covered.push(key);
            waits.push(earlier.done);
         } else {
            refetch.push(key);
            dropCachedSchema(conn, key);
         }
      }
      const answers: Promise<R>[] = [];
      if (refetch.length > 0) {
         const refetching = fetch(refetch, true);
         const done = refetching.catch(() => undefined);
         for (const key of refetch) recent.set(key, { startedAt: now, done });
         answers.push(refetching);
      }
      if (covered.length > 0) {
         answers.push(
            Promise.allSettled(waits).then(() => fetch(covered, false)),
         );
      }
      return Promise.all(answers);
   }
}

/** Drop `key` from a Malloy connection's schema cache, if it has one. */
function dropCachedSchema(conn: object, key: string): void {
   const cache = (conn as { schemaCache?: unknown }).schemaCache;
   if (cache !== null && typeof cache === "object") {
      delete (cache as Record<string, unknown>)[key];
   }
}
