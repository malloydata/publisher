// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One mutex per key ({@link KeyedLock}), for the run's in-process locks: the
 * package's table lock (table_claims.ts) and the owned-name lock
 * (table_ownership.ts). Their order is in run/README.md.
 */

import { Mutex } from "async-mutex";

/** A mutex per key, removed once nobody holds or waits for it. */
export class KeyedLock {
   private readonly locks = new Map<
      string,
      { mutex: Mutex; holders: number }
   >();

   /** Take the lock on `key`; the release it returns is idempotent. */
   async acquire(key: string): Promise<() => void> {
      let lock = this.locks.get(key);
      if (!lock) {
         lock = { mutex: new Mutex(), holders: 0 };
         this.locks.set(key, lock);
      }
      // Counted before the wait, so a waiter keeps the mutex in the map.
      lock.holders++;
      const held = lock;
      const release = await held.mutex.acquire();
      let released = false;
      return () => {
         if (released) return;
         released = true;
         release();
         if (--held.holders === 0) this.locks.delete(key);
      };
   }

   async withLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
      const release = await this.acquire(key);
      try {
         return await fn();
      } finally {
         release();
      }
   }

   /** Keys with a holder or a waiter. */
   get size(): number {
      return this.locks.size;
   }
}
