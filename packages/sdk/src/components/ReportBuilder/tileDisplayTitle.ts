// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { fallbackTileTitle, tileTitle } from "../Dashboard/tileTitle";
import { tileViewExpression, type QueryTile } from "./document";

/**
 * The title a query tile is drawn under when it has no label of its own, the
 * one the reader gives it: a query the builder named for a view is titled as
 * that view, any other query by its own name, a view tile by its view. One
 * definition, so a live tile, a placeholder and the menu agree.
 */
export const tileDisplayTitle = (tile: QueryTile): string =>
   tile.label ??
   (tile.declaration.kind === "named"
      ? fallbackTileTitle(tile.name, tileViewExpression(tile))
      : tileTitle(tileViewExpression(tile)));
