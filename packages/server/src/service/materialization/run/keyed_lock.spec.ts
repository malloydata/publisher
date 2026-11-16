// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { KeyedLock } from "./keyed_lock";
import { TableClaims } from "./table_claims";
import { withOwnedNameLock } from "./table_ownership";

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
      const lock = new KeyedLock();
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
      const lock = new KeyedLock();
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
      const lock = new KeyedLock();
      await expect(
         lock.withLock("k", async () => {
            throw new Error("boom");
         }),
      ).rejects.toThrow("boom");
      expect(lock.size).toBe(0);
   });
});

/** Hold the owned-name lock on `name` until the returned release is called. */
async function holdOwnedName(name: string): Promise<() => void> {
   let release!: () => void;
   const done = new Promise<void>((resolve) => (release = resolve));
   let entered!: () => void;
   const held = new Promise<void>((resolve) => (entered = resolve));
   void withOwnedNameLock(name, async () => {
      entered();
      await done;
   });
   await held;
   return release;
}

describe("the owned-name lock", () => {
   it("serializes spellings of one name that differ in case, schema or quoting", async () => {
      for (const other of [
         "SUMMARY",
         "summary",
         "analytics.summary",
         'analytics."summary"',
      ]) {
         const release = await holdOwnedName('analytics."Summary"');
         const waiting = withOwnedNameLock(other, async () => {});
         expect(await settled(waiting)).toBe(false);
         release();
         await waiting;
      }
   });

   it("lets builds at different names through", async () => {
      const release = await holdOwnedName("analytics.summary");
      const other = withOwnedNameLock("analytics.details", async () => {});
      expect(await settled(other)).toBe(true);
      release();
   });

   it("comes last in the lock order: the table lock is refused under it, under test", async () => {
      const claims = new TableClaims(() => ({}) as never);
      await expect(
         withOwnedNameLock("analytics.summary", () =>
            claims.withTableLock("env", "pkg", async () => {}),
         ),
      ).rejects.toThrow(/Lock order/);
      // Taken in order, or once the hold is done, it is fine.
      await claims.withTableLock("env", "pkg", () =>
         withOwnedNameLock("analytics.summary", async () => {}),
      );
   });
});
