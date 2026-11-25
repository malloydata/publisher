// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Tooltip } from "@mui/material";
import { StatusPill } from "./StatusPill";
import { HeaderChip } from "./headerCard";

/**
 * A package whose root `index.malloy` decides what it publishes hides every other
 * model file: the models list leaves it out and the model GET answers 404. People
 * who can edit the package still need to read those files, so for them the app asks
 * with `includeHiddenFilesAndSources`, which a host allows only for someone who can edit
 * the package. The same flag rides on their runs of such a file, and Publisher then drops
 * the surface limit for that run, so an editor can query a hidden source too. Anyone else
 * never sends the flag, so for them a hidden file stays a 404.
 */

/** The marker on a hidden file, in the package's models list and on its page. */
export const OFF_SURFACE_LABEL = "Private";

/** What the marker means: its tooltip, wherever it shows. */
export const OFF_SURFACE_DETAIL =
   "index.malloy doesn't export this file, so only people who can edit the package see it.";

/** Whether a models-list entry is a file the package hides. Absent (an older server) reads as not hidden. */
export function isHidden(model: { isHidden?: boolean }): boolean {
   return model.isHidden === true;
}

/**
 * The Private marker on a row of the package's models list: not a tab stop, since the row is
 * one link and a control inside it would be a second. Its why is in the tooltip for a pointer,
 * and in the row's accessible name for everyone else ({@link surfaceRowLabel}), as a row's
 * access pills put theirs (AccessChips `accessRowLabel`).
 */
export function OffSurfacePill() {
   return (
      <Tooltip title={OFF_SURFACE_DETAIL}>
         <StatusPill tone="warning" data-testid="model-viewer-off-surface">
            {OFF_SURFACE_LABEL}
         </StatusPill>
      </Tooltip>
   );
}

/**
 * The Private marker on a hidden file's own page (its source's status row, or above the code):
 * a status-row chip like its siblings (HeaderChip), so it is a tab stop whose accessible name
 * is its why, reachable by keyboard and read by a screen reader, not only shown on hover.
 */
export function OffSurfaceChip() {
   return (
      <HeaderChip
         title={`${OFF_SURFACE_LABEL}: ${OFF_SURFACE_DETAIL}`}
         testId="model-viewer-off-surface"
      >
         <StatusPill tone="warning" size="header">
            {OFF_SURFACE_LABEL}
         </StatusPill>
      </HeaderChip>
   );
}

/** The marker on the package's root `index.malloy`: the sources it exports are the package's public ones. */
export const PUBLIC_SURFACE_LABEL = "Public";

/** What the marker means on the root `index.malloy`'s row in the package's models list. */
export const PUBLIC_FILE_DETAIL =
   "What index.malloy exports is what the package publishes: everyone who can read the package can query its sources.";

/** What the marker means in a source's status row on the root `index.malloy`'s page. */
export const PUBLIC_SOURCE_DETAIL =
   "index.malloy exports this source, so everyone who can read the package can query it.";

/**
 * The marker on the package's root `index.malloy`'s row in the models list: what it exports is
 * what the package publishes, so it is the model a reader should open first. Green beside the
 * amber Private on the files it leaves out ({@link OffSurfacePill}), so the two read as one
 * public/private pair (the guide's §4 names this pair as the one Declaration that takes a
 * tone), and drawn the same way: not a tab stop, its why in the tooltip and in the row's
 * accessible name ({@link surfaceRowLabel}).
 */
export function PublicSurfacePill() {
   return (
      <Tooltip title={PUBLIC_FILE_DETAIL}>
         <StatusPill
            tone="positive"
            data-testid="model-viewer-public-surface"
            sx={{ alignSelf: "center" }}
         >
            {PUBLIC_SURFACE_LABEL}
         </StatusPill>
      </Tooltip>
   );
}

/**
 * The Public marker in a source's status row on the root `index.malloy`'s page: a status-row
 * chip like its siblings (HeaderChip), a tab stop whose accessible name is its why.
 */
export function PublicSurfaceChip() {
   return (
      <HeaderChip
         title={`${PUBLIC_SURFACE_LABEL}: ${PUBLIC_SOURCE_DETAIL}`}
         testId="model-viewer-public-surface"
      >
         <StatusPill tone="positive" size="header">
            {PUBLIC_SURFACE_LABEL}
         </StatusPill>
      </HeaderChip>
   );
}

/** Which side of the package's surface a models-list row's file is on, when it is marked. */
export type FileSurface = "public" | "private";

/** The words a models-list row's accessible name adds for its surface pill (", Private: …"). */
export function surfaceRowLabel(surface: FileSurface | undefined): string {
   if (surface === "public")
      return `, ${PUBLIC_SURFACE_LABEL}: ${PUBLIC_FILE_DETAIL}`;
   if (surface === "private")
      return `, ${OFF_SURFACE_LABEL}: ${OFF_SURFACE_DETAIL}`;
   return "";
}
