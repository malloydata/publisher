import { describe, expect, it } from "bun:test";
import { SchemaCache } from "./schema_cache";

describe("SchemaCache", () => {
   it("evicts the least recently used entry, counting a read as a use", () => {
      const cache = new SchemaCache<string>(2);
      cache.set("a", "A", 1);
      cache.set("b", "B", 1);
      expect(cache.get("a")).toBe("A");
      cache.set("c", "C", 1);
      expect(cache.get("b")).toBeUndefined();
      expect(cache.get("a")).toBe("A");
      expect(cache.get("c")).toBe("C");
      expect(cache.size).toBe(2);
   });

   it("misses an entry older than the request's refreshTimestamp, and drops it", () => {
      const cache = new SchemaCache<string>(10);
      cache.set("t", "old", 100);
      expect(cache.get("t", 100)).toBe("old");
      expect(cache.get("t", 101)).toBeUndefined();
      expect(cache.get("t")).toBeUndefined();
   });

   it("stores nothing when sized to zero", () => {
      const cache = new SchemaCache<string>(0);
      cache.set("t", "T", 1);
      expect(cache.get("t")).toBeUndefined();
      expect(cache.size).toBe(0);
   });

   it("counts hits, including a caller that joins a fetch under way", async () => {
      const cache = new SchemaCache<string>(10);
      cache.set("t", "T", 1);
      cache.get("t");
      cache.get("missing");
      let resolve: (v: string) => void = () => {};
      cache.trackInFlight("u", new Promise<string>((r) => (resolve = r)));
      const joined = cache.joinInFlight("u");
      expect(cache.hits).toBe(2);
      resolve("U");
      expect(await joined).toBe("U");
   });

   it("resolves a joined fetch that failed to undefined, and forgets it once settled", async () => {
      const cache = new SchemaCache<string>(10);
      const failing = Promise.reject(new Error("warehouse down"));
      cache.trackInFlight("t", failing);
      const joined = cache.joinInFlight("t");
      expect(await joined).toBeUndefined();
      await Promise.resolve();
      await Promise.resolve();
      expect(cache.joinInFlight("t")).toBeUndefined();
   });
});
