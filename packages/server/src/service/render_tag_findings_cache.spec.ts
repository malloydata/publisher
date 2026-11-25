// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { RenderTagWarning } from "./model";
import {
   RENDER_VALIDATOR_VERSION,
   RenderTagFindingsCache,
   renderTagFindingsKey,
} from "./render_tag_findings_cache";

const FINDING: RenderTagWarning = {
   subject: "s -> v",
   message: "Tag 'big_value' is only valid on basic fields",
   severity: "error",
};

describe("RenderTagFindingsCache", () => {
   it("hits on the same digest and validator, and misses on another of either", () => {
      const cache = new RenderTagFindingsCache(8);
      cache.set(renderTagFindingsKey("d1", "0.0.438"), [FINDING]);
      expect(cache.get(renderTagFindingsKey("d1", "0.0.438"))).toEqual([
         FINDING,
      ]);
      expect(cache.get(renderTagFindingsKey("d2", "0.0.438"))).toBeUndefined();
      expect(cache.get(renderTagFindingsKey("d1", "0.0.439"))).toBeUndefined();
   });

   it("keys on the installed validator by default", () => {
      expect(RENDER_VALIDATOR_VERSION).toMatch(/^\d+\.\d+\.\d+/);
      expect(renderTagFindingsKey("d")).toBe(
         renderTagFindingsKey("d", RENDER_VALIDATOR_VERSION),
      );
      expect(renderTagFindingsKey("d")).not.toBe(
         renderTagFindingsKey("d", `${RENDER_VALIDATOR_VERSION}-next`),
      );
   });

   it("caches a clean model's empty findings as a hit", () => {
      const cache = new RenderTagFindingsCache(8);
      cache.set("k", []);
      expect(cache.get("k")).toEqual([]);
   });

   it("hands out copies, so a caller's edit never reaches the cache", () => {
      const cache = new RenderTagFindingsCache(8);
      const findings = [{ ...FINDING }];
      cache.set("k", findings);
      findings[0].message = "edited after set";
      const first = cache.get("k")!;
      first[0].message = "edited after get";
      first.push({ ...FINDING });
      expect(cache.get("k")).toEqual([FINDING]);
   });

   it("holds at most its bound, dropping the least recently used", () => {
      const cache = new RenderTagFindingsCache(2);
      cache.set("a", []);
      cache.set("b", []);
      expect(cache.get("a")).toEqual([]); // a is now the most recent
      cache.set("c", []);
      expect(cache.size).toBe(2);
      expect(cache.get("b")).toBeUndefined();
      expect(cache.get("a")).toEqual([]);
      expect(cache.get("c")).toEqual([]);
   });
});
