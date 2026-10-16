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

   it("names the served view, declares no source, and leaves a notebook tile without a width", () => {
      const draft = document();
      addTileToDocument(draft, tile, { ...context, widths: false });
      expect(draft.sources).toEqual([]);
      expect(draft.tiles).toEqual([
         {
            name: "by_day",
            source: "orders",
            declaration: { kind: "inherited" },
         },
      ]);
   });

   it("makes a tile with a width, title or chart a query of the report's own", () => {
      const draft = document();
      addTileToDocument(
         draft,
         { ...tile, label: "Daily", chart: "bar_chart" },
         context,
      );
      expect(draft.sources).toEqual([]);
      expect(draft.tiles).toEqual([
         {
            name: "by_day_tile",
            source: "orders",
            declaration: { kind: "named", from: "by_day" },
            colspan: 6,
            label: "Daily",
            chart: "bar_chart",
         },
      ]);
   });

   it("makes a second tile on a view already shown a query, so the two can be told apart", () => {
      const draft = document();
      addTileToDocument(draft, { ...tile, colspan: undefined }, context);
      addTileToDocument(draft, { ...tile, colspan: undefined }, context);
      expect(draft.tiles).toEqual([
         {
            name: "by_day",
            source: "orders",
            declaration: { kind: "inherited" },
         },
         {
            name: "by_day_tile",
            source: "orders",
            declaration: { kind: "named", from: "by_day" },
         },
      ]);
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

   it("copies a view this file declares as a query of the report's own over the same view, right after it", () => {
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
         declaration: { kind: "named", from: "by_day" },
         label: "Daily",
         chart: "bar_chart",
         colspan: 6,
         filters: [{ field: "region", given: "REGION" }],
      });
   });

   it("copies a served view as a query of the report's own over it, with its settings, declaring no source", () => {
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
      expect(draft.sources).toEqual([]);
      expect(draft.tiles[1]).toEqual({
         name: "by_brand_tile",
         source: "orders",
         declaration: { kind: "named", from: "by_brand" },
         label: "From the model",
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

describe("duplicateTile beside a narrowed extension", () => {
   it("copies a served view onto that view, never onto an extension that narrows it", () => {
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
      expect(draft.tiles[1]).toMatchObject({
         source: "orders",
         declaration: { kind: "named", from: "by_brand" },
      });
      expect(draft.sources.map((s) => s.name)).toEqual(["orders_west"]);
   });
});
