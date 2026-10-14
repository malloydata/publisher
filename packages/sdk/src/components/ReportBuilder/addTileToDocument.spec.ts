// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { NewTile } from "./AddTileDialog";
import {
   addTileToDocument,
   duplicateBlocked,
   duplicateTile,
   type AddTileContext,
} from "./addTileToDocument";
import type { DashboardDocument } from "./document";

const document = (): DashboardDocument => ({
   layout: "grid",
   title: "X",
   imports: [{ kind: "all", from: "../b.malloy" }],
   sources: [],
   tiles: [],
});

// `orders` is exported by both models; the file whole-imports the second.
const tile: NewTile = {
   base: "orders",
   modelPath: "a.malloy",
   exporters: ["a.malloy", "b.malloy"],
   view: "by_day",
   colspan: 6,
};
const context: AddTileContext = {
   modelPath: "dashboards/x.malloy",
   textHeld: false,
   widths: true,
};

describe("addTileToDocument", () => {
   it("adds no import when a whole-file import reaches any exporter of the source", () => {
      const draft = document();
      addTileToDocument(draft, tile, context);
      expect(draft.imports).toEqual([{ kind: "all", from: "../b.malloy" }]);
   });

   it("imports from the crediting model when no exporter is reached", () => {
      const draft = { ...document(), imports: [] };
      addTileToDocument(draft, tile, context);
      expect(draft.imports).toEqual([
         { kind: "names", names: ["orders"], from: "../a.malloy" },
      ]);
   });

   it("writes no import for a document held as text", () => {
      const draft = { ...document(), imports: [] };
      addTileToDocument(draft, tile, { ...context, textHeld: true });
      expect(draft.imports).toEqual([]);
   });

   it("puts the tile on a new extension of its base, and leaves a notebook tile without a width", () => {
      const draft = document();
      addTileToDocument(draft, tile, { ...context, widths: false });
      expect(draft.sources).toEqual([{ name: "orders_tiles", base: "orders" }]);
      expect(draft.tiles).toHaveLength(1);
      expect(draft.tiles[0]).not.toHaveProperty("colspan");
   });
});

describe("duplicateTile", () => {
   const withTiles = (
      tiles: DashboardDocument["tiles"],
   ): DashboardDocument => ({
      ...document(),
      sources: [{ name: "orders_tiles", base: "orders" }],
      tiles,
   });

   it("copies a view this file declares, as a view of its own right after it", () => {
      const draft = withTiles([
         {
            name: "by_day_tile",
            source: "orders_tiles",
            declaration: { kind: "reference", from: "by_day" },
            label: "Daily",
            chart: "bar_chart",
            colspan: 6,
            break: true,
            filters: [{ field: "region", given: "REGION" }],
         },
      ]);
      expect(duplicateTile(draft, 0)).toBe(1);
      expect(draft.tiles[1]).toEqual({
         name: "by_day_tile_2",
         source: "orders_tiles",
         declaration: { kind: "reference", from: "by_day" },
         label: "Daily",
         chart: "bar_chart",
         colspan: 6,
         filters: [{ field: "region", given: "REGION" }],
      });
   });

   it("copies a view the model declares onto the file's extension of its source", () => {
      const draft: DashboardDocument = {
         ...document(),
         tiles: [
            {
               name: "by_brand",
               source: "orders",
               declaration: { kind: "inherited" as const },
               label: "From the model",
               colspan: 4,
            },
         ],
      };
      expect(duplicateTile(draft, 0)).toBe(1);
      expect(draft.sources).toEqual([{ name: "orders_tiles", base: "orders" }]);
      expect(draft.tiles[1]).toEqual({
         name: "by_brand_tile",
         source: "orders_tiles",
         declaration: { kind: "reference", from: "by_brand" },
         colspan: 4,
      });
   });

   it("refuses a view whose body the writer would have to copy, and says why", () => {
      const inline = {
         name: "t",
         source: "orders_tiles",
         declaration: { kind: "inline" as const },
      };
      const draft = withTiles([inline]);
      expect(duplicateBlocked(inline)).toContain("does not copy");
      expect(duplicateTile(draft, 0)).toBeUndefined();
      expect(draft.tiles).toHaveLength(1);
   });
});

describe("duplicateTile onto a narrowed extension", () => {
   it("copies a model view onto an unnarrowed extension, never a scoped one", () => {
      const draft: DashboardDocument = {
         ...document(),
         sources: [
            { name: "orders_west", base: "orders", scopedBy: ["REGION"] },
         ],
         tiles: [
            {
               name: "by_brand",
               source: "orders",
               declaration: { kind: "inherited" },
            },
         ],
      };
      expect(duplicateTile(draft, 0)).toBe(1);
      expect(draft.tiles[1]).toMatchObject({ source: "orders_tiles" });
      expect(draft.sources.map((s) => s.name)).toEqual([
         "orders_west",
         "orders_tiles",
      ]);
   });
});
