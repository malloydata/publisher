// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { NewTile } from "./AddTileDialog";
import { addTileToDocument, type AddTileContext } from "./addTileToDocument";
import type { DashboardDocument } from "./document";

const document = (): DashboardDocument => ({
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
   notebook: false,
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
      addTileToDocument(draft, tile, { ...context, notebook: true });
      expect(draft.sources).toEqual([{ name: "orders_tiles", base: "orders" }]);
      expect(draft.tiles).toHaveLength(1);
      expect(draft.tiles[0]).not.toHaveProperty("colspan");
   });
});
