// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A bounded, least-recently-used cache of the table and SQL schemas a
 * package-load worker has fetched through the main thread.
 *
 * Every model compiles separately, so one load asks for the same table many
 * times; caching removes the round trip to the main thread without changing
 * when the warehouse is queried.
 *
 * Bounded by serialized bytes rather than entry count, so a few very wide
 * tables cannot take an unbounded share of the heap. Keys must name everything
 * the schema depends on (environment, connection instance, table path or
 * SQL). Entries never go stale on their own; a `refreshTimestamp` request
 * bypasses the cache and overwrites the entry.
 */
export class SchemaCache<T> {
   private readonly entries = new Map<string, { value: T; bytes: number }>();
   private readonly inFlight = new Map<string, Promise<InFlightAnswer<T>>>();
   private totalBytes = 0;
   private hitCount = 0;

   /**
    * `maxBytes` bounds the summed `sizeOf` of the entries; 0 disables the
    * cache. `sizeOf` is measured once, when an entry is stored.
    */
   constructor(
      private readonly maxBytes: number,
      private readonly sizeOf: (value: T) => number = (value) =>
         JSON.stringify(value)?.length ?? 0,
   ) {}

   get(key: string): T | undefined {
      const entry = this.entries.get(key);
      if (!entry) return undefined;
      // Re-insert so iteration order is least-recently-used first.
      this.entries.delete(key);
      this.entries.set(key, entry);
      this.hitCount += 1;
      return entry.value;
   }

   set(key: string, value: T): void {
      this.delete(key);
      if (this.maxBytes <= 0) return;
      const bytes = this.sizeOf(value);
      // One entry larger than the whole budget would evict everything else.
      if (bytes > this.maxBytes) return;
      this.entries.set(key, { value, bytes });
      this.totalBytes += bytes;
      while (this.totalBytes > this.maxBytes) {
         const oldest = this.entries.keys().next().value;
         if (oldest === undefined) break;
         this.delete(oldest);
      }
   }

   private delete(key: string): void {
      const entry = this.entries.get(key);
      if (!entry) return;
      this.entries.delete(key);
      this.totalBytes -= entry.bytes;
   }

   get size(): number {
      return this.entries.size;
   }

   /** The summed size of the entries held, as measured by `sizeOf`. */
   get bytes(): number {
      return this.totalBytes;
   }

   /** Hits since the cache was created; callers diff it around a load. */
   get hits(): number {
      return this.hitCount;
   }

   /**
    * The answer of the fetch already under way for `key`, if any. A caller
    * answered with a value records that with {@link noteJoinedHit}.
    */
   joinInFlight(key: string): Promise<InFlightAnswer<T>> | undefined {
      return this.inFlight.get(key);
   }

   /** Count a request answered by a fetch it joined, as a hit. */
   noteJoinedHit(): void {
      this.hitCount += 1;
   }

   /**
    * Record `fetch` as under way for `key` until it settles, so concurrent
    * misses for the same key share one fetch and its outcome, an error
    * included. The value is cached by the caller with {@link set}; a fetch that
    * rejects answers with its message as the error.
    */
   trackInFlight(key: string, fetch: Promise<InFlightAnswer<T>>): void {
      const settled = fetch.catch(
         (error: unknown): InFlightAnswer<T> => ({
            error: error instanceof Error ? error.message : String(error),
         }),
      );
      this.inFlight.set(key, settled);
      void settled.finally(() => {
         if (this.inFlight.get(key) === settled) this.inFlight.delete(key);
      });
   }
}

/** What a fetch produced for one key: its value, or why there is none. */
export interface InFlightAnswer<T> {
   value?: T;
   error?: string;
}
