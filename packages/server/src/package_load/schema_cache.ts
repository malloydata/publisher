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
 * Entries outlive a load and are evicted least-recently-used first. Keys are
 * built by the caller and must name everything the schema depends on: the
 * environment, the connection and its digest, and the table path or SQL.
 * Freshness follows Malloy's own connection cache: an entry older than the
 * request's `refreshTimestamp` is a miss.
 */
export class SchemaCache<T> {
   private readonly entries = new Map<
      string,
      { value: T; timestamp: number }
   >();
   private hitCount = 0;

   constructor(private readonly maxEntries: number) {}

   get(key: string, refreshTimestamp?: number): T | undefined {
      const entry = this.entries.get(key);
      if (!entry) return undefined;
      if (
         refreshTimestamp !== undefined &&
         refreshTimestamp > entry.timestamp
      ) {
         this.entries.delete(key);
         return undefined;
      }
      // Re-insert so iteration order is least-recently-used first.
      this.entries.delete(key);
      this.entries.set(key, entry);
      this.hitCount += 1;
      return entry.value;
   }

   set(key: string, value: T, timestamp: number): void {
      if (this.maxEntries <= 0) return;
      this.entries.delete(key);
      this.entries.set(key, { value, timestamp });
      while (this.entries.size > this.maxEntries) {
         const oldest = this.entries.keys().next().value;
         if (oldest === undefined) break;
         this.entries.delete(oldest);
      }
   }

   get size(): number {
      return this.entries.size;
   }

   /** Hits since the cache was created; callers diff it around a load. */
   get hits(): number {
      return this.hitCount;
   }

   private readonly inFlight = new Map<string, Promise<T | undefined>>();

   /**
    * The fetch already under way for `key`, if any. A caller that joins one
    * is counted as a hit: it is answered without a request of its own. The
    * promise resolves to undefined when that fetch did not produce `key`.
    */
   joinInFlight(key: string): Promise<T | undefined> | undefined {
      const pending = this.inFlight.get(key);
      if (pending) this.hitCount += 1;
      return pending;
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
