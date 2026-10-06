// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

export { Dashboard, type DashboardProps } from "./Dashboard";
export {
   DashboardView,
   STICKY_CONTROLS_Z,
   type DashboardViewProps,
} from "./DashboardView";
export { TileFilterTag, tileIgnoredFilterLabels } from "./TileFilterTag";
export { DashboardTile, type DashboardTileProps } from "./DashboardTile";
export {
   documentPreamble,
   RESTRICTED_NOTICE,
   withPreamble,
} from "./textSource";
export type { DashboardEvent, DashboardEventHandler } from "./telemetry";
export { TILE_MIN_HEIGHT, TileCard, TileHeading } from "./TileCard";
export type { TileHeadingSlots } from "./TileCard";
