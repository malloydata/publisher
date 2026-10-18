// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { NewTile } from "./AddTileDialog";
import {
   isInlineStage,
   isQueryTile,
   type DashboardDocument,
   type QueryTile,
} from "./document";
import { DEFAULT_COLUMNS } from "../Dashboard/DashboardGrid";
import { withSource } from "./imports";
import { freeQueryName, needsQuery, normalizeTileForms } from "./tileForm";
import { rowsOf, settle } from "./placement";

/** Where a tile is added and what the document is: the parts of an add that are not the tile. */
export interface AddTileContext {
   /** The document's file within the package, which a tile's import is relative to. */
   modelPath: string | undefined;
   /** The document is held as text: it has no `import`, so a tile's source is the run model's and nothing is imported. */
   textHeld: boolean;
   /** Whether tiles carry a width (`LayoutRules.widths`): off, an added tile takes none. */
   widths: boolean;
   /** Where the tile lands; undefined appends. */
   insertAt?: number;
}

/**
 * A tile from the picker, added to a draft: an entry naming the served view,
 * `"flights -> kpis"`, or a query of the report's own over it when the picker
 * gave it a width, title or chart ({@link normalizeTileForms}). No source is
 * declared for it.
 */
export function addTileToDocument(
   draft: DashboardDocument,
   tile: NewTile,
   { modelPath, textHeld, widths, insertAt }: AddTileContext,
): void {
   const before = structuredClone(draft);
   // A source the file cannot see yet comes in by name, with the tile.
   if (tile.modelPath && !textHeld)
      draft.imports = withSource(
         draft,
         tile.base,
         tile.modelPath,
         modelPath,
         tile.exporters ?? [tile.modelPath],
      );
   draft.tiles.splice(insertAt ?? draft.tiles.length, 0, {
      name: tile.view,
      source: tile.base,
      declaration: { kind: "inherited" },
      ...(widths && tile.colspan !== undefined
         ? { colspan: tile.colspan }
         : {}),
      ...(tile.label ? { label: tile.label } : {}),
      ...(tile.chart ? { chart: tile.chart } : {}),
      ...(tile.chart && tile.chartReset?.length
         ? { chartReset: tile.chartReset }
         : {}),
   });
   normalizeTileForms(draft, before);
}

/**
 * Why a tile cannot be duplicated, or undefined when it can. A copy is a query
 * of the report's own over the same view, so a tile whose body the builder
 * does not write has no copy it could write.
 */
export function duplicateBlocked(tile: QueryTile): string | undefined {
   switch (tile.declaration.kind) {
      case "inline":
         return "Its query is written in this file, and the builder does not copy a query's body.";
      case "opaque":
         return `Its query is ${tile.declaration.why}, which the builder does not copy.`;
      case "query":
         return "It names a query the builder does not edit, and a copy could not be told apart from it.";
      default:
         if (tile.chart === "custom")
            return "Its chart line is one the builder does not write, so a copy would lose it.";
         return tile.unmodelled
            ? "Its query says more than the builder writes, so a copy would lose it."
            : undefined;
   }
}

/**
 * A copy of the query tile at `index`, placed right after it: a query of the
 * report's own over the view the original shows, with every setting the
 * original has but its row break, so its title, chart and width can then
 * differ. Returns where the copy landed, or undefined when it cannot be copied
 * ({@link duplicateBlocked}).
 *
 * The view a copy reads is the one the original shows: an `inherited` tile's
 * view, or the view a `named` query or a `reference` view in an extension
 * (a file written before the builder wrote queries) is declared from, on the
 * same source. Never the original's own name, which would carry the
 * original's tags and break when the original is removed.
 */
export function duplicateTile(
   draft: DashboardDocument,
   index: number,
): number | undefined {
   const original = draft.tiles[index];
   if (!original || !isQueryTile(original)) return undefined;
   if (duplicateBlocked(original) !== undefined) return undefined;
   if (original.source === undefined) return undefined;
   const view =
      original.declaration.kind === "named" ||
      original.declaration.kind === "reference"
         ? original.declaration.from
         : original.name;
   const {
      break: _break,
      name: _name,
      declaration: _declaration,
      source,
      // Read from the original's own lines, which a copy does not carry.
      ownPalette: _ownPalette,
      unmodelled: _unmodelled,
      chartLines: _chartLines,
      ...settings
   } = structuredClone(original);
   // A copy with nothing of its own is a second entry naming the same view;
   // one with settings is a query of its own. An inline stage has no view to
   // name, so its copy is always a query, named after the original.
   const inline = original.declaration.kind === "named" && isInlineStage(view);
   let copy: QueryTile = {
      ...settings,
      name: freeQueryName(draft, inline ? original.name : view),
      source,
      declaration: { kind: "named", from: view },
   };
   if (!inline && !needsQuery(copy))
      copy = {
         ...settings,
         name: view,
         source,
         declaration: { kind: "inherited" },
      };
   // On a placed report the copy goes straight under the original, at its
   // size, and whatever was there moves down; it never lands on top of it.
   const placed = original.row !== undefined && original.col !== undefined;
   if (placed) {
      copy = {
         ...copy,
         row: (original.row ?? 1) + rowsOf(original),
         col: original.col,
      };
   }
   draft.tiles.splice(index + 1, 0, copy);
   if (placed)
      draft.tiles = settle(
         draft.tiles,
         draft.columns ?? DEFAULT_COLUMNS,
         index + 1,
      );
   return index + 1;
}
