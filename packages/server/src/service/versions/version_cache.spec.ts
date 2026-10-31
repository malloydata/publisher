// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { VersionCache, VersionEvictedDuringLoadError } from "./version_cache";

interface Loaded {
   id: string;
}

/** A cache whose loads wait until released, recording loads and releases. */
function harness() {
   const loads: string[] = [];
   const released: string[] = [];
   const gates = new Map<
      string,
      { open: () => void; fail: (e: Error) => void }
   >();
   const cache = new VersionCache<Loaded>({
      load: (pkg, v) => {
         const id = `${pkg}@${v}`;
         loads.push(id);
         return new Promise<Loaded>((resolve, reject) => {
            gates.set(id, {
               open: () => resolve({ id }),
               fail: reject,
            });
         });
      },
      release: (_pkg, _v, loaded) => released.push(loaded.id),
   });
   const open = async (id: string) => {
      // Let the load register its gate before opening it.
      for (let i = 0; i < 10 && !gates.has(id); i++) await Promise.resolve();
      gates.get(id)!.open();
      gates.delete(id);
   };
   const fail = async (id: string, error: Error) => {
      for (let i = 0; i < 10 && !gates.has(id); i++) await Promise.resolve();
      gates.get(id)!.fail(error);
      gates.delete(id);
   };
   return { cache, loads, released, open, fail };
}

describe("VersionCache", () => {
   it("shares one load between concurrent reads of a version", async () => {
      const { cache, loads, open } = harness();
      const reads = [cache.get("p", "1.0.0"), cache.get("p", "1.0.0")];
      await open("p@1.0.0");
      const [a, b] = await Promise.all(reads);
      expect(a).toBe(b);
      expect(loads).toEqual(["p@1.0.0"]);
      expect(await cache.get("p", "1.0.0")).toBe(a);
      expect(loads).toEqual(["p@1.0.0"]);
   });

   it("loads different versions in parallel, each served on its own", async () => {
      const { cache, loads, open } = harness();
      const one = cache.get("p", "1.0.0");
      const two = cache.get("p", "2.0.0");
      await Promise.resolve();
      expect(loads).toEqual(["p@1.0.0", "p@2.0.0"]);
      await open("p@2.0.0");
      expect((await two).id).toBe("p@2.0.0");
      expect(cache.isLoaded("p", "1.0.0")).toBe(false);
      await open("p@1.0.0");
      expect((await one).id).toBe("p@1.0.0");
      expect(
         cache
            .entries()
            .map(([pkg, v]) => `${pkg}@${v}`)
            .sort(),
      ).toEqual(["p@1.0.0", "p@2.0.0"]);
   });

   it("does not cache a failed load, so the next read tries again", async () => {
      const { cache, loads, fail, open } = harness();
      const first = cache.get("p", "1.0.0");
      await fail("p@1.0.0", new Error("compile failed"));
      await expect(first).rejects.toThrow("compile failed");
      const second = cache.get("p", "1.0.0");
      await open("p@1.0.0");
      expect((await second).id).toBe("p@1.0.0");
      expect(loads).toEqual(["p@1.0.0", "p@1.0.0"]);
   });

   it("evict releases a loaded version and the next read loads it again", async () => {
      const { cache, released, open } = harness();
      const first = cache.get("p", "1.0.0");
      await open("p@1.0.0");
      await first;
      cache.evict("p", "1.0.0");
      expect(released).toEqual(["p@1.0.0"]);
      expect(cache.peek("p", "1.0.0")).toBeUndefined();
      cache.evict("p", "1.0.0");
      expect(released).toEqual(["p@1.0.0"]);
   });

   it("a load that finishes after an evict is released, never served", async () => {
      const { cache, released, open } = harness();
      const inFlight = cache.get("p", "1.0.0");
      cache.evict("p", "1.0.0");
      await open("p@1.0.0");
      await expect(inFlight).rejects.toBeInstanceOf(
         VersionEvictedDuringLoadError,
      );
      expect(released).toEqual(["p@1.0.0"]);
      expect(cache.isLoaded("p", "1.0.0")).toBe(false);
   });

   it("evictPackage takes every version of one package, loaded or loading", async () => {
      const { cache, released, open } = harness();
      const a = cache.get("p", "1.0.0");
      await open("p@1.0.0");
      await a;
      const other = cache.get("q", "1.0.0");
      await open("q@1.0.0");
      await other;
      const loading = cache.get("p", "2.0.0");

      cache.evictPackage("p");
      await open("p@2.0.0");
      await expect(loading).rejects.toBeInstanceOf(
         VersionEvictedDuringLoadError,
      );
      expect(released.sort()).toEqual(["p@1.0.0", "p@2.0.0"]);
      expect(cache.isLoaded("q", "1.0.0")).toBe(true);
   });

   it("a read after an evict starts its own load instead of joining the overtaken one", async () => {
      // Archive then unarchive while the first load is still running: the
      // next read must load afresh and succeed, not inherit the stale failure.
      const loads: string[] = [];
      const pending: ((value: Loaded) => void)[] = [];
      const released: string[] = [];
      const cache = new VersionCache<Loaded>({
         load: (pkg, v) => {
            loads.push(`${pkg}@${v}`);
            return new Promise<Loaded>((resolve) => pending.push(resolve));
         },
         release: (_pkg, _v, loaded) => released.push(loaded.id),
      });
      const overtaken = cache.get("p", "1.0.0");
      await Promise.resolve();
      cache.evict("p", "1.0.0");
      const fresh = cache.get("p", "1.0.0");
      await Promise.resolve();
      expect(loads).toEqual(["p@1.0.0", "p@1.0.0"]);

      // The old load finishes first and must not clear the new one's slot.
      pending[0]({ id: "old" });
      await expect(overtaken).rejects.toBeInstanceOf(
         VersionEvictedDuringLoadError,
      );
      expect(released).toEqual(["old"]);
      const joined = cache.get("p", "1.0.0");
      expect(loads).toHaveLength(2);

      pending[1]({ id: "new" });
      expect((await fresh).id).toBe("new");
      expect((await joined).id).toBe("new");
      expect(cache.peek("p", "1.0.0")?.id).toBe("new");
   });

   it("does not keep a load that throws before it awaits anything", async () => {
      let calls = 0;
      const cache = new VersionCache<Loaded>({
         load: (pkg, v) => {
            calls += 1;
            if (calls === 1) throw new Error("synchronous failure");
            return Promise.resolve({ id: `${pkg}@${v}` });
         },
         release: () => {},
      });
      await expect(cache.get("p", "1.0.0")).rejects.toThrow(
         "synchronous failure",
      );
      expect((await cache.get("p", "1.0.0")).id).toBe("p@1.0.0");
   });
});
