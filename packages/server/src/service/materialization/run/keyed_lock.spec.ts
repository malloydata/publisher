// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { tableIdentity } from "../../versions/materialization_scope";
import { KeyedLock } from "./keyed_lock";
import { acquireOwnedNameLock, ownedNameLockKey } from "./table_ownership";

/** Whether `promise` has settled by the time pending callbacks have run. */
async function settled(promise: Promise<unknown>): Promise<boolean> {
   let done = false;
   void promise.then(
      () => (done = true),
      () => (done = true),
   );
   await new Promise((resolve) => setTimeout(resolve, 0));
   return done;
}

describe("KeyedLock", () => {
   it("serializes holders of one key and lets other keys through", async () => {
      const lock = new KeyedLock({ name: "a" });
      const release = await lock.acquire("k");
      const sameKey = lock.acquire("k");
      const otherKey = lock.acquire("other");
      expect(await settled(sameKey)).toBe(false);
      expect(await settled(otherKey)).toBe(true);
      release();
      expect(await settled(sameKey)).toBe(true);
      (await sameKey)();
      (await otherKey)();
   });

   it("keeps a key's mutex while anyone holds or waits for it, and no longer", async () => {
      const lock = new KeyedLock({ name: "a" });
      const first = await lock.acquire("k");
      const waiting = lock.acquire("k");
      expect(lock.size).toBe(1);
      first();
      // A second release is a no-op, not a release of the waiter's hold.
      first();
      const second = await waiting;
      expect(lock.size).toBe(1);
      second();
      expect(lock.size).toBe(0);
      await lock.withLock("k", async () => {
         expect(lock.size).toBe(1);
      });
      expect(lock.size).toBe(0);
   });

   it("releases the key when the work under it throws", async () => {
      const lock = new KeyedLock({ name: "a" });
      await expect(
         lock.withLock("k", async () => {
            throw new Error("boom");
         }),
      ).rejects.toThrow("boom");
      expect(lock.size).toBe(0);
   });

   it("refuses, under test, a lock taken while one that comes after it is held", async () => {
      const later = new KeyedLock({ name: "later" });
      const earlier = new KeyedLock({
         name: "earlier",
         takenBefore: ["later"],
      });
      // In order: fine.
      await earlier.withLock("k", () => later.withLock("k", async () => {}));
      // Inverted: refused before it waits.
      await expect(
         later.withLock("k", () => earlier.withLock("k", async () => {})),
      ).rejects.toThrow(/Lock order/);
      expect(earlier.size).toBe(0);
      expect(later.size).toBe(0);
   });
});

describe("the owned-name lock's key", () => {
   it("serializes two spellings of one name that differ only in case or schema", async () => {
      expect(ownedNameLockKey("analytics.Summary")).toBe(
         ownedNameLockKey("summary"),
      );
      const release = await acquireOwnedNameLock("analytics.Summary");
      const other = acquireOwnedNameLock("SUMMARY");
      expect(await settled(other)).toBe(false);
      release();
      (await other)();
   });

   // A quoted spelling and a bare one can name the same table (Postgres folds
   // a bare `summary` to the table `"summary"`), and tableIdentity reads them
   // as one; the owned-name lock does not strip quotes, so it gives them two
   // keys and two builds at that name are not serialized. Pinned as found.
   it("gives a quoted spelling and a bare one of one table different keys", async () => {
      expect(tableIdentity('"summary"')).toBe(tableIdentity("summary"));
      expect(ownedNameLockKey('"summary"')).not.toBe(
         ownedNameLockKey("summary"),
      );
      const quoted = await acquireOwnedNameLock('analytics."summary"');
      const bare = acquireOwnedNameLock("analytics.summary");
      expect(await settled(bare)).toBe(true);
      quoted();
      (await bare)();
   });
});
