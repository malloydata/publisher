import { describe, expect, it } from "bun:test";
import { SchemaCache } from "./schema_cache";

/** Sizes an entry by its string length, so a test can reason in bytes. */
const byLength = (value: string): number => value.length;

describe("SchemaCache", () => {
   it("evicts the least recently used entries to stay within its byte budget", () => {
      const cache = new SchemaCache<string>(6, byLength);
      cache.set("a", "AAA", 1);
      cache.set("b", "BBB", 1);
      expect(cache.get("a")).toBe("AAA");
      cache.set("c", "CC", 1);
      expect(cache.get("b")).toBeUndefined();
      expect(cache.get("a")).toBe("AAA");
      expect(cache.get("c")).toBe("CC");
      expect(cache.bytes).toBe(5);
   });

   it("does not store an entry larger than the whole budget, and keeps the rest", () => {
      const cache = new SchemaCache<string>(4, byLength);
      cache.set("small", "SS", 1);
      cache.set("huge", "HHHHHHHH", 1);
      expect(cache.get("huge")).toBeUndefined();
      expect(cache.get("small")).toBe("SS");
   });

   it("replacing an entry releases the bytes of the one it replaces", () => {
      const cache = new SchemaCache<string>(10, byLength);
      cache.set("t", "TTTTTT", 1);
      cache.set("t", "TT", 2);
      expect(cache.bytes).toBe(2);
      expect(cache.size).toBe(1);
   });

   it("misses an entry older than the request's refreshTimestamp, and drops it", () => {
      const cache = new SchemaCache<string>(100, byLength);
      cache.set("t", "old", 100);
      expect(cache.get("t", 100)).toBe("old");
      expect(cache.get("t", 101)).toBeUndefined();
      expect(cache.get("t")).toBeUndefined();
      expect(cache.bytes).toBe(0);
   });

   it("stores nothing when its budget is zero", () => {
      const cache = new SchemaCache<string>(0, byLength);
      cache.set("t", "T", 1);
      expect(cache.get("t")).toBeUndefined();
      expect(cache.size).toBe(0);
   });

   it("measures entries by their serialized size by default", () => {
      const cache = new SchemaCache<{ name: string }>(1000);
      cache.set("t", { name: "orders" }, 1);
      expect(cache.bytes).toBe(JSON.stringify({ name: "orders" }).length);
   });

   it("counts a joined fetch as a hit only when the caller says it was answered", async () => {
      const cache = new SchemaCache<string>(100, byLength);
      cache.set("t", "T", 1);
      cache.get("t");
      cache.get("missing");
      let resolve: (v: string) => void = () => {};
      cache.trackInFlight("u", new Promise<string>((r) => (resolve = r)));
      const joined = cache.joinInFlight("u");
      expect(cache.hits).toBe(1);
      resolve("U");
      expect(await joined).toBe("U");
      cache.noteJoinedHit();
      expect(cache.hits).toBe(2);
   });

   it("resolves a joined fetch that failed to undefined, and forgets it once settled", async () => {
      const cache = new SchemaCache<string>(100, byLength);
      cache.trackInFlight("t", Promise.reject(new Error("warehouse down")));
      expect(await cache.joinInFlight("t")).toBeUndefined();
      await Promise.resolve();
      await Promise.resolve();
      expect(cache.joinInFlight("t")).toBeUndefined();
   });
});
