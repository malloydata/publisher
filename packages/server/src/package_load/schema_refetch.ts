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
 * refetching after this load was requested takes that refetch's answer,
 * rather than being fetched again or read back from the connection's cache
 * (which an older, unrefreshed fetch may have overwritten meanwhile).
 */
export class SchemaRefetches<A> {
   private readonly byConnection = new WeakMap<
      object,
      Map<string, { startedAt: number; answer: Promise<A | undefined> }>
   >();

   /**
    * `maxAgeMs`: how long a refetch and its answer are kept, at least as long
    * as a load can wait and run. `now` is a monotonic clock in ms, the one
    * load request times are read on.
    */
   constructor(
      private readonly maxAgeMs: number,
      private readonly now: () => number = () => performance.now(),
   ) {}

   /**
    * Answer `keys` (`conn`'s schema-cache keys) for a refreshing load
    * requested at `requestedAt`: a key a refetch begun since then covers
    * takes that refetch's answer; the rest have their entries dropped and are
    * refetched together by `refetch`, whose map answers each key (a key it
    * leaves out answers `undefined`).
    */
   async fetch(
      conn: object,
      keys: string[],
      requestedAt: number,
      refetch: (keys: string[]) => Promise<Map<string, A>>,
   ): Promise<Map<string, A | undefined>> {
      const now = this.now();
      let recent = this.byConnection.get(conn);
      if (!recent) {
         recent = new Map();
         this.byConnection.set(conn, recent);
      }
      for (const [key, earlier] of recent) {
         if (now - earlier.startedAt > this.maxAgeMs) recent.delete(key);
      }
      const answers = new Map<string, Promise<A | undefined>>();
      const toRefetch: string[] = [];
      for (const key of new Set(keys)) {
         const earlier = recent.get(key);
         if (earlier && earlier.startedAt >= requestedAt) {
            answers.set(key, earlier.answer);
         } else {
            toRefetch.push(key);
            dropCachedSchema(conn, key);
         }
      }
      if (toRefetch.length > 0) {
         const refetching = refetch(toRefetch);
         for (const key of toRefetch) {
            const answer = refetching.then((byKey) => byKey.get(key));
            // A failed refetch is answered to its own load below; a load
            // sharing it gets the same failure.
            answer.catch(() => undefined);
            answers.set(key, answer);
            recent.set(key, { startedAt: now, answer });
         }
      }
      const settled = await Promise.all(
         [...answers].map(
            async ([key, answer]) => [key, await answer] as const,
         ),
      );
      return new Map(settled);
   }
}
/** Drop `key` from a Malloy connection's schema cache, if it has one. */
function dropCachedSchema(conn: object, key: string): void {
   const cache = (conn as { schemaCache?: unknown }).schemaCache;
   if (cache !== null && typeof cache === "object") {
      delete (cache as Record<string, unknown>)[key];
   }
}
