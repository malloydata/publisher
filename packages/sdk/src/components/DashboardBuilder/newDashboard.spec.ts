// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { newDashboardSource, slugFor } from "./newDashboard";
import { openDocument } from "./testing/fixtures";

describe("a new dashboard", () => {
   it("names its file after its title", () => {
      expect(slugFor("Sales by Region")).toBe("sales-by-region");
      expect(slugFor("  Q3 — West (draft) ")).toBe("q3-west-draft");
      expect(slugFor("***")).toBe("");
   });

   it("is a file the builder opens, with the picked view as its first tile", async () => {
      const text = newDashboardSource({
         title: 'Sales "West"',
         modelPath: "models/storefront.malloy",
         source: "order_items",
         view: "by_category",
      });
      expect(text).toContain('title="Sales \\"West\\""');
      expect(text).toContain(
         'import { order_items } from "../models/storefront.malloy"',
      );
      const document = await openDocument(text);
      expect(document.title).toBe('Sales "West"');
      expect(document.sources).toEqual([
         { name: "order_items_tiles", base: "order_items" },
      ]);
      expect(document.tiles.map((tile) => tile.name)).toEqual([
         "by_category_tile",
      ]);
      expect(document.tiles[0].declaration).toEqual({
         kind: "reference",
         from: "by_category",
      });
      expect(document.tiles[0].colspan).toBe(6);
   });
});
