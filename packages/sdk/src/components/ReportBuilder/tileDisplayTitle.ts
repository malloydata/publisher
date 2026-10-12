// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { tileTitle } from "../Dashboard/DashboardTile";
import type { QueryTile } from "./document";

/**
 * The title a query tile is drawn under when it has no label of its own: the
 * one the reader's view derives from the view's name. One definition, so a
 * live tile, a placeholder and the menu agree.
 */
export const tileDisplayTitle = (tile: QueryTile): string =>
   tile.label ?? tileTitle(`${tile.source} -> ${tile.name}`);
