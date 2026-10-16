// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { tileTitle } from "../Dashboard/DashboardTile";
import { tileViewExpression, type QueryTile } from "./document";

/**
 * The title a query tile is drawn under when it has no label of its own: the
 * one the reader's view derives from the name of the view it shows (a query
 * of the report's own shows its view's). One definition, so a
 * live tile, a placeholder and the menu agree.
 */
export const tileDisplayTitle = (tile: QueryTile): string =>
   tile.label ?? tileTitle(tileViewExpression(tile));
