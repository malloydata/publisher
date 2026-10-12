// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { changedTileKey } from "./changedTile";
import type { DashboardDocument, DashboardTile } from "./document";

const query = (name: string, extra: Partial<DashboardTile> = {}) =>
   ({
      name,
      source: "s",
      declaration: { kind: "reference", from: name },
      ...extra,
   }) as DashboardTile;
const doc = (...tiles: DashboardTile[]): DashboardDocument =>
   ({ title: "T", sources: [], tiles }) as unknown as DashboardDocument;

describe("changedTileKey", () => {
   it("names an edited tile", () => {
      const before = doc(query("a"), query("b"));
      const after = doc(query("a"), query("b", { label: "B" }));
      expect(changedTileKey(before, after)).toBe("s.b");
   });

   it("names an added tile, and a text tile by its own key", () => {
      const before = doc(query("a"));
      const after = doc(query("a"), {
         kind: "text",
         name: "text_1",
         markdown: "hi",
      });
      expect(changedTileKey(before, after)).toBe("text.text_1");
   });

   it("names the first tile out of its place after a move", () => {
      const before = doc(query("a"), query("b"), query("c"));
      const after = doc(query("b"), query("a"), query("c"));
      expect(changedTileKey(before, after)).toBe("s.b");
   });

   it("names nothing when only the page changed, or a tile went away", () => {
      const before = doc(query("a"), query("b"));
      expect(changedTileKey(before, { ...before, title: "U" })).toBeUndefined();
      expect(changedTileKey(before, doc(query("a")))).toBeUndefined();
   });
});
