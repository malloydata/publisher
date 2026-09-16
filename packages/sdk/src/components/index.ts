// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Exported because Dashboard, DashboardTile and Notebook all present request
// failures through it; a host composing those needs the same presentation for
// its own.
export { ApiErrorDisplay, type ApiErrorDisplayProps } from "./ApiErrorDisplay";
// The Console's button roles; see `buttons.tsx`. Exported so a host — the
// Publisher app included — builds its own screens out of the same four.
export { AddButton, SecondaryButton } from "./buttons";
export { AppDialog } from "./AppDialog";
export { BackLink } from "./BackLink";
export { DashboardBar } from "./Dashboard/DashboardBar";
// The Console's palette, and the two maps drawn from it. Exported so a host
// paints its own chrome — the Publisher app's theme included — from the same
// hues rather than picking a near-miss beside them.
export { PALETTE, SURFACE_TINT } from "./styles";
export { useRouterClickHandler, type NavigationClick } from "./click_helper";
export * from "./Dashboard";
export * from "./DataAppViewer";
export * from "./drill";
export * from "./Environment";
export * from "./filter";
export * from "./given";
export * from "./Home";
export * from "./Loading";
export * from "./Materializations";
export * from "./Model";
export * from "./Notebook";
export * from "./Package";
export {
   Prose,
   type ProseLinkContext,
   type ProseProps,
   type ProseVariant,
} from "./Prose";
export * from "./QueryResult";
export * from "./RenderedResult";
export { ServerProvider, useServer } from "./ServerProvider";
export type { ServerContextValue, ServerProviderProps } from "./ServerProvider";
export * from "./DocumentStorage";
