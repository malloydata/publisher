// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A bounded, least-recently-used cache of the table and SQL schemas a
 * package-load worker has fetched through the main thread.
 *
 * Each worker compiles one package at a time, and a package compiles every
 * model separately, so the same table is asked for many times within a load
 * (160 requests for 36 tables in one observed package) and again by every
 * later load of a package on the same tables. Each request is a round trip
 * through the main thread, which answers it from its connection's own schema
 * cache and copies the schema back. Caching here removes the round trip; it
 * changes nothing about when a schema is fetched from the warehouse.
 *
 * Entries outlive a load and are evicted least-recently-used first, to keep
 * their total serialized size within a byte budget: an entry count would let a
 * few very wide tables take an unbounded share of the worker's heap. Keys are
 * built by the caller and must name everything the schema depends on: the
 * environment, the connection instance, and the table path or SQL.
 * Freshness follows Malloy's own connection cache: an entry older than the
 * request's `refreshTimestamp` is a miss.
 */
export class SchemaCache<T> {
   private readonly entries = new Map<
      string,
      { value: T; timestamp: number; bytes: number }
   >();
   private readonly inFlight = new Map<string, Promise<T | undefined>>();
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

   get(key: string, refreshTimestamp?: number): T | undefined {
      const entry = this.entries.get(key);
      if (!entry) return undefined;
      if (
         refreshTimestamp !== undefined &&
         refreshTimestamp > entry.timestamp
      ) {
         this.delete(key);
         return undefined;
      }
      // Re-insert so iteration order is least-recently-used first.
      this.entries.delete(key);
      this.entries.set(key, entry);
      this.hitCount += 1;
      return entry.value;
   }

   set(key: string, value: T, timestamp: number): void {
      this.delete(key);
      if (this.maxBytes <= 0) return;
      const bytes = this.sizeOf(value);
      // One entry larger than the whole budget would evict everything else.
      if (bytes > this.maxBytes) return;
      this.entries.set(key, { value, timestamp, bytes });
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
    * The fetch already under way for `key`, if any. The promise resolves to
    * undefined when that fetch did not produce `key`. A caller answered by it
    * records that with {@link noteJoinedHit}.
    */
   joinInFlight(key: string): Promise<T | undefined> | undefined {
      return this.inFlight.get(key);
   }

   /** Count a request answered by a fetch it joined, as a hit. */
   noteJoinedHit(): void {
      this.hitCount += 1;
   }

   /**
    * Record `fetch` as under way for `key` until it settles, so concurrent
    * misses for the same key share one fetch. The value is cached by the
    * caller with {@link set}; a fetch that fails resolves to undefined here.
    */
   trackInFlight(key: string, fetch: Promise<T | undefined>): void {
      const settled = fetch.catch(() => undefined);
      this.inFlight.set(key, settled);
      void settled.finally(() => {
         if (this.inFlight.get(key) === settled) this.inFlight.delete(key);
      });
   }
}
