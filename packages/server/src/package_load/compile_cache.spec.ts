// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ModelDef } from "@malloydata/malloy";
import { describe, expect, it, spyOn } from "bun:test";
import { logger } from "../logger";
import { PackageCompileCache } from "./compile_cache";

/** Every file reads as unchanged, so a stored entry hits until it is dropped. */
const reader = {
   readURL: async () => "",
   getInvalidationKey: async () => "k",
};

const url = (name: string) => `file:///pkg/${name}.malloy`;

async function store(cache: PackageCompileCache, name: string) {
   await cache.manager.setCachedModelDef(url(name), {
      modelDef: { dependencies: {} } as unknown as ModelDef,
      invalidationKeys: { [url(name)]: "k" },
   });
}

const held = async (cache: PackageCompileCache, name: string) =>
   (await cache.manager.getCachedModelDef(reader, url(name))) !== undefined;

describe("PackageCompileCache", () => {
   it("drops every entry when one more would pass the cap, and logs that once", async () => {
      const warn = spyOn(logger, "warn").mockImplementation(() => logger);
      try {
         const cache = new PackageCompileCache("env/pkg", 2);
         await store(cache, "a");
         await store(cache, "b");
         await store(cache, "a"); // replacing an entry is not growth
         expect([await held(cache, "a"), await held(cache, "b")]).toEqual([
            true,
            true,
         ]);
         await store(cache, "c");
         expect([
            await held(cache, "a"),
            await held(cache, "b"),
            await held(cache, "c"),
         ]).toEqual([false, false, true]);
         await store(cache, "d");
         await store(cache, "e");
         expect(warn).toHaveBeenCalledTimes(1);
      } finally {
         warn.mockRestore();
      }
   });

   it("empties itself when a load's stamp differs from the last one", async () => {
      const cache = new PackageCompileCache("env/pkg");
      cache.beginLoad("1\0data-a");
      await store(cache, "a");
      cache.beginLoad("1\0data-a");
      expect(await held(cache, "a")).toBe(true);
      cache.beginLoad("1\0data-b");
      expect(await held(cache, "a")).toBe(false);
   });

   it("never serves the top-level file to a compile that translates it, but serves its imports", async () => {
      const cache = new PackageCompileCache("env/pkg");
      await store(cache, "a");
      await store(cache, "b");
      const translating = cache.forModel(new URL(url("a")), {
         translate: true,
      });
      expect(await translating.getCachedModelDef(reader, url("a"))).toBe(
         undefined,
      );
      expect(
         await translating.getCachedModelDef(reader, url("b")),
      ).toBeDefined();
      expect(translating.hit).toBe(false);
      const serving = cache.forModel(new URL(url("a")), { translate: false });
      expect(await serving.getCachedModelDef(reader, url("a"))).toBeDefined();
      expect(serving.hit).toBe(true);
   });
});
