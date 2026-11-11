// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { CatalogView, PackageCatalog } from "./catalog";
import type { DashboardDocument, QueryTile } from "./document";

/**
 * The catalog's view behind a tile: a view this file declares, read off the
 * file's own compiled model, else the base source's view the tile names (a
 * served view, or the view a query of the report's own refines). Undefined for
 * an inline or opaque body, which is no view of the base source, and for an
 * inline stage, whose shape only its result says.
 */
export function catalogViewOfTile(
   tile: QueryTile,
   document: DashboardDocument,
   catalog: PackageCatalog | undefined,
): CatalogView | undefined {
   if (!catalog) return undefined;
   const declared = catalog.documentViews
      ?.get(tile.source ?? "")
      ?.get(tile.name);
   if (declared && document.sources.some((s) => s.name === tile.source))
      return declared;
   if (tile.declaration.kind === "inline" || tile.declaration.kind === "opaque")
      return undefined;
   const base =
      document.sources.find((s) => s.name === tile.source)?.base ?? tile.source;
   const viewName =
      tile.declaration.kind === "reference" || tile.declaration.kind === "named"
         ? tile.declaration.from
         : tile.name;
   return catalog.sources
      .find((s) => s.name === base)
      ?.views.find((v) => v.name === viewName);
}
