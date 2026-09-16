// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Card, CardContent, CardMedia, styled } from "@mui/material";

/**
 * The Console's palette: one set of hues, used everywhere a colour carries
 * meaning.
 *
 * These are the register data tools encode categories in — saturated, mid-dark,
 * evenly spaced around the wheel — rather than colours lifted from a logo. A
 * logo is drawn to be recognised at one size in one place; a palette has to
 * distinguish a dozen kinds of thing in a list, behind a glyph, and along a
 * chart's series, and the two jobs pull in different directions. The previous
 * palette was the logo's three plus two families invented to extend it, which
 * is how one screen ended up with three unrelated colour systems on it.
 *
 * **Every value clears 3:1 against white**, WCAG's minimum for a graphical
 * object, because these are painted as small solid backplates behind white
 * glyphs where the glyph has to stay readable. That is a hard constraint on
 * anything added here, asserted in `ContentTypeIcon.spec.ts` rather than left
 * to review: a too-light plate looks perfectly fine in a screenshot and simply
 * stops being legible. Measured against white, lowest first: emerald 3.8:1,
 * red 4.8:1, lime 5.0:1, amber 5.0:1, blue 5.2:1, orange 5.2:1, cyan 5.4:1,
 * teal 5.5:1, violet 5.7:1, pink 6.0:1, indigo 6.3:1, slate 7.6:1.
 */
export const PALETTE = {
   blue: "#2563eb",
   indigo: "#4f46e5",
   violet: "#7c3aed",
   pink: "#be185d",
   red: "#dc2626",
   orange: "#c2410c",
   amber: "#b45309",
   lime: "#4d7c0f",
   emerald: "#059669",
   teal: "#0f766e",
   cyan: "#0e7490",
   slate: "#475569",
} as const;

/**
 * The tints for what a SERVER holds, as against what a package holds. Three
 * kinds — an environment, a package, a connection — listed on the home and
 * environment pages.
 *
 * Kept clear of the hues `CONTENT_TINT` uses, so a colour means one kind of
 * thing across the whole Console rather than one kind per page. All three are
 * real colours rather than near-neutrals: a grey plate reads as chrome instead
 * of as one of the coloured kinds, which is the plate's whole job.
 *
 * Also kept clear of the primary. Packages were indigo, which sat a few degrees
 * from the blue of the "Package" button on the same row and read as a failed
 * attempt at the same colour. Teal is the furthest hue from blue still open,
 * and packages and connections — the pair that actually share a screen — now
 * sit on opposite sides of the wheel. Environments never appear beside either.
 */
export const SURFACE_TINT = {
   environment: PALETTE.emerald,
   package: PALETTE.teal,
   connection: PALETTE.orange,
} as const;

/**
 * Monospace font stack used by code-like surfaces inside the SDK
 * (file-path labels in `ItemRow`, code blocks, etc.). Matches the
 * `MONO_FONT_FAMILY` defined in the publisher app's theme.
 */
export const MONO_FONT_FAMILY =
   '"JetBrains Mono", "ui-monospace", "SFMono-Regular", "Menlo", monospace';

export const StyledCard = styled(Card)({
   display: "flex",
   flexDirection: "column",
   height: "100%",
   boxShadow: "none",
   border: "none",
   backgroundColor: "transparent",
});

export const StyledCardContent = styled(CardContent)({
   display: "flex",
   flexDirection: "column",
   padding: "0",
   flexGrow: 1,
});

export const StyledCardMedia = styled(CardMedia)({
   padding: "0",
});

// New clean notebook styles
export const CleanNotebookContainer = styled("div")(({ theme }) => ({
   backgroundColor: theme.palette.background.default,
   padding: "0 8px 0px 8px",
   borderRadius: "12px",
   boxShadow: "none",
   border: "none",
   maxWidth: "1200px",
   margin: "0 auto",
}));

export const CleanNotebookSection = styled("div")({
   marginBottom: "48px",
   padding: "0",
   backgroundColor: "transparent",
   border: "none",
   boxShadow: "none",
});

export const CleanMetricCard = styled("div")({
   backgroundColor: "transparent",
   paddingTop: "12px",
   paddingBottom: "2px",
   borderRadius: "8px",
   border: "none",
   boxShadow: "none",
   marginBottom: "0",
});

export const StyledExplorerPage = styled("div")({
   height: "100%",
});

export const StyledExplorerContent = styled("div")({
   height: "75vh",
   width: "100%",
   overflowY: "auto",
});

// Package page styles
