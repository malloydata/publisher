// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One mutex per key ({@link KeyedLock}), for the run's in-process locks: the
 * package's table lock (table_claims.ts) and the lock on builds at one
 * author's `name=` (table_ownership.ts). A key's mutex lives only while a
 * holder or a waiter uses it. In-process: one Publisher owns its store.
 */

import { AsyncLocalStorage } from "node:async_hooks";
import { Mutex } from "async-mutex";
import { logger } from "../../../logger";
import { externalEnv } from "../../../settings/external";

/**
 * The keyed locks taken through withLock in this async context, each marked
 * released once its holder is done: work its holder started without waiting
 * for it (a background run) inherits the context but not the hold.
 */
const heldLocks = new AsyncLocalStorage<
   readonly { name: string; held: boolean }[]
>();

/** A mutex per key, removed once nobody holds or waits for it. */
export class KeyedLock {
   private readonly locks = new Map<
      string,
      { mutex: Mutex; holders: number }
   >();

   constructor(
      private readonly options: {
         /** The lock's name in the lock order (run/README.md). */
         name: string;
         /**
          * The names of the locks this one is taken before in the lock order:
          * none may be held, in this async context, when this one is taken.
          * Checked against holds taken through {@link withLock} only.
          */
         takenBefore?: readonly string[];
      },
   ) {}

   /**
    * Wait for, and take, the lock on `key`. Returns the release, which may be
    * called more than once; only the first call releases.
    */
   async acquire(key: string): Promise<() => void> {
      this.assertOrder();
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

   /** Run `fn` holding the lock on `key`. */
   async withLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
      const release = await this.acquire(key);
      const hold = { name: this.options.name, held: true };
      try {
         return await heldLocks.run(
            [...(heldLocks.getStore() ?? []), hold],
            fn,
         );
      } finally {
         hold.held = false;
         release();
      }
   }

   /** How many keys have a holder or a waiter. */
   get size(): number {
      return this.locks.size;
   }

   /**
    * A lock taken while one that comes after it is held can deadlock against
    * a caller taking them in order. Thrown under test, logged otherwise.
    */
   private assertOrder(): void {
      const held = heldLocks.getStore() ?? [];
      const inverted = this.options.takenBefore?.find((name) =>
         held.some((hold) => hold.held && hold.name === name),
      );
      if (inverted === undefined) return;
      const message = `Lock order: the ${this.options.name} lock was taken while the ${inverted} lock, which comes after it, was held`;
      if (externalEnv("NODE_ENV") === "test") throw new Error(message);
      logger.error(message);
   }
}
