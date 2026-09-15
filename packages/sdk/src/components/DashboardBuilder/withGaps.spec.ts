// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { withGaps } from "./DashboardBuilder";
import type { DashboardTile } from "./document";

const tile = (
   name: string,
   colspan: number,
   starts = false,
): DashboardTile => ({
   name,
   source: "a",
   declaration: { kind: "reference", from: `${name}_view` },
   colspan,
   ...(starts ? { break: true } : {}),
});

/** The grid's entries as a reader would list them: a name, or a gap and its width. */
const shape = (entries: ReturnType<typeof withGaps>) =>
   entries.map((entry) =>
      entry.kind === "gap"
         ? `gap(${entry.colspan}) after ${entry.after}`
         : entry.tile.name,
   );

describe("withGaps", () => {
   it("lays a gap where a break leaves a row short", () => {
      // The storefront overview with the trend narrowed: half a row open to
      // its right, because `kpis` breaks onto a fresh row.
      const entries = withGaps(
         [tile("trend", 6), tile("kpis", 6, true), tile("map", 6)],
         12,
      );
      expect(shape(entries)).toEqual([
         "trend",
         "gap(6) after a.trend",
         "kpis",
         "map",
      ]);
   });

   it("lays a gap where a tile is too wide for what is left of the row", () => {
      // No break anywhere: the wide tile wraps of its own accord, and the
      // space it could not fit into is a gap all the same.
      const entries = withGaps([tile("narrow", 4), tile("wide", 12)], 12);
      expect(shape(entries)).toEqual([
         "narrow",
         "gap(8) after a.narrow",
         "wide",
      ]);
   });

   it("lays a gap at the end of the last row", () => {
      const entries = withGaps(
         [tile("one", 6), tile("two", 6), tile("three", 6)],
         12,
      );
      expect(shape(entries)).toEqual([
         "one",
         "two",
         "three",
         "gap(6) after a.three",
      ]);
   });

   it("lays no gap into a full row", () => {
      const entries = withGaps(
         [tile("one", 6), tile("two", 6), tile("three", 12)],
         12,
      );
      expect(entries.every((entry) => entry.kind === "tile")).toBe(true);
   });

   it("numbers tiles by their place among the tiles, not among the entries", () => {
      // The sortable is told a tile's index; a gap between two tiles must not
      // shift the second one's.
      const entries = withGaps([tile("one", 6), tile("two", 6, true)], 12);
      const indices = entries.flatMap((entry) =>
         entry.kind === "tile" ? [entry.index] : [],
      );
      expect(indices).toEqual([0, 1]);
   });

   it("carries each tile's width and row start through to the grid", () => {
      const [first, , second] = withGaps(
         [tile("one", 6), tile("two", 4, true)],
         12,
      );
      expect(first.colspan).toBe(6);
      expect(first.break).toBeUndefined();
      expect(second.colspan).toBe(4);
      expect(second.break).toBe(true);
   });
});
