// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { EMPTY_SNAPSHOT, pin, type Pinned } from "../domain/binding";
import { VariantCompileCache } from "./variant_compile_cache";

type V = { value?: number; error?: string };
const failed = (error: string): V => ({ error });

/** A pin over its own bindings array. */
const pinnedOver = (): Pinned => {
   const p = pin(EMPTY_SNAPSHOT, new Date(0));
   return { ...p, snapshot: { ...p.snapshot, bindings: [] } };
};

describe("VariantCompileCache", () => {
   it("compiles a key once and shares the compile in flight", async () => {
      const cache = new VariantCompileCache<V>();
      let calls = 0;
      let release!: (v: V) => void;
      const compile = () => {
         calls++;
         return new Promise<V>((r) => (release = r));
      };
      const a = cache.get("k", compile, failed);
      const b = cache.get("k", compile, failed);
      release({ value: 1 });
      expect(await a).toEqual({ value: 1 });
      expect(await b).toBe(await a);
      expect(calls).toBe(1);
   });

   it("keeps a failed compile as its failed value, not retried", async () => {
      const cache = new VariantCompileCache<V>();
      let calls = 0;
      const compile = async (): Promise<V> => {
         calls++;
         throw new Error("does not compile");
      };
      expect(await cache.get("k", compile, failed)).toEqual({
         error: "does not compile",
      });
      expect(await cache.get("k", compile, failed)).toEqual({
         error: "does not compile",
      });
      expect(calls).toBe(1);
   });

   it("invalidate drops every variant and starts a new generation", async () => {
      const cache = new VariantCompileCache<V>();
      await cache.get("k", async () => ({ value: 1 }), failed);
      const before = cache.generation;
      cache.invalidate();
      expect(cache.generation).toBe(before + 1);
      expect(cache.size).toBe(0);
      expect(await cache.get("k", async () => ({ value: 2 }), failed)).toEqual({
         value: 2,
      });
   });

   it("starts over only when a query is pinned to other bindings", async () => {
      const cache = new VariantCompileCache<V>();
      const first = pinnedOver();
      cache.over(first);
      await cache.get("k", async () => ({ value: 1 }), failed);
      const generation = cache.generation;
      cache.over(first);
      expect(cache.generation).toBe(generation);
      expect(cache.size).toBe(1);
      cache.over(pinnedOver());
      expect(cache.generation).toBe(generation + 1);
      expect(cache.size).toBe(0);
   });

   it("drops the least recently used variant past its limit", async () => {
      const cache = new VariantCompileCache<V>(2);
      let calls = 0;
      const compile = async (): Promise<V> => ({ value: ++calls });
      await cache.get("a", compile, failed);
      await cache.get("b", compile, failed);
      await cache.get("a", compile, failed); // a is now the most recent
      await cache.get("c", compile, failed); // drops b
      expect(cache.size).toBe(2);
      expect(await cache.get("a", compile, failed)).toEqual({ value: 1 });
      expect(await cache.get("b", compile, failed)).toEqual({ value: 4 });
   });

   it("hands each compile a scope of its own, a compile again after a drop included", async () => {
      const cache = new VariantCompileCache<V & { scope?: string }>(1);
      const scopes: string[] = [];
      const compile = async (scope: string) => {
         scopes.push(scope);
         return { scope };
      };
      await cache.get("a", compile, failed);
      await cache.get("b", compile, failed); // drops a
      await cache.get("a", compile, failed); // a again, compiled anew
      cache.invalidate();
      await cache.get("a", compile, failed);
      expect(new Set(scopes).size).toBe(4);
   });
});
