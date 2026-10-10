// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { keepRowStructure } from "./layout";
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

/** The break flags a row structure is made of, read back off a tile list. */
const rows = (tiles: DashboardTile[]) =>
   tiles.map((each) => each.break ?? false);
const names = (tiles: DashboardTile[]) => tiles.map((each) => each.name);

describe("keepRowStructure", () => {
   // Four half-width tiles on a twelve-column grid are a 2x2, and the whole
   // point is that moving one leaves it a 2x2.
   const quad = [
      tile("one", 6),
      tile("two", 6, true),
      tile("three", 6),
      tile("four", 6, true),
   ];

   it("keeps the row starts where they were when a tile moves", () => {
      const moved = [...quad];
      const [taken] = moved.splice(1, 1);
      moved.splice(2, 0, taken);

      const settled = keepRowStructure(moved, rows(quad));

      expect(names(settled)).toEqual(["one", "three", "two", "four"]);
      // The pattern is unchanged, so the grid is still two rows of two: the
      // tile that left did not carry its row start along with it.
      expect(rows(settled)).toEqual(rows(quad));
   });

   it("gives the row start to whichever tile lands on it", () => {
      const moved = [...quad];
      const [taken] = moved.splice(0, 1);
      moved.splice(1, 0, taken);

      const settled = keepRowStructure(moved, rows(quad));

      expect(names(settled)).toEqual(["two", "one", "three", "four"]);
      // `two` carried the break and has given it up on moving off that slot;
      // `one` has taken it on by arriving there.
      expect(settled[0].break).toBeUndefined();
      expect(settled[1].break).toBe(true);
   });

   it("leaves widths with their tiles", () => {
      const mixed = [tile("wide", 12), tile("narrow", 4, true)];
      const moved = [mixed[1], mixed[0]];

      const settled = keepRowStructure(moved, rows(mixed));

      // A width is genuinely the tile's, so it travels; only the row start is
      // positional.
      expect(settled.map((each) => [each.name, each.colspan])).toEqual([
         ["narrow", 4],
         ["wide", 12],
      ]);
      expect(rows(settled)).toEqual([false, true]);
   });

   it("returns the same tile object when nothing about it changes", () => {
      const settled = keepRowStructure([...quad], rows(quad));
      // No needless copies: an untouched tile is the tile it was, which is what
      // keeps the editor's own "did this change anything" check honest.
      expect(settled[0]).toBe(quad[0]);
      expect(settled[1]).toBe(quad[1]);
   });
});
