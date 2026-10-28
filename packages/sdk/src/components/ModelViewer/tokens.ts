// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PALETTE } from "../styles";
import { MONO_FONT_FAMILY } from "./theme";

/**
 * Design tokens for the model viewer: its accents and its type scale, drawn
 * from the SDK's palette so the viewer reads as part of the same Console.
 */

export const ink = "text.primary";

export type AccentName = "steel" | "sage" | "brown" | "olive" | "amber";

/** An accent hue: `main` for a fill or glyph, `tint` behind it, `onTint` on top of `tint`. */
export interface Accent {
   main: string;
   tint: string;
   onTint: string;
}

const accent = (main: string): Accent => ({
   main,
   tint: `color-mix(in srgb, ${main} 12%, transparent)`,
   onTint: main,
});

export const ACCENT: Record<AccentName, Accent> = {
   steel: accent(PALETTE.cyan),
   sage: accent(PALETTE.lime),
   brown: accent(PALETTE.slate),
   olive: accent(PALETTE.violet),
   amber: accent(PALETTE.amber),
};

/**
 * Accent per field kind: steel = dimensions, sage = measures, amber =
 * calculations, olive = views. Joins render brown (see SchemaJoin).
 */
export function kindAccent(
   kind: "dimension" | "measure" | "calculate" | "view",
): Accent {
   if (kind === "measure") return ACCENT.sage;
   if (kind === "calculate") return ACCENT.amber;
   if (kind === "view") return ACCENT.olive;
   return ACCENT.steel;
}

/** Deterministic per-source accent: cycle the palette by source index. */
const SOURCE_ACCENT_CYCLE: AccentName[] = ["steel", "brown", "olive", "sage"];
export function sourceAccentName(index: number): AccentName {
   return SOURCE_ACCENT_CYCLE[index % SOURCE_ACCENT_CYCLE.length];
}

/** The type scale the rows are set in. */
export const FONT_SIZE = { body: 14, small: 13, caption: 12 } as const;

/**
 * The identifier style for a CONTAINER row (a join): a thing you open to find
 * more things. Mono, because it exists verbatim in a .malloy file. One step
 * above the leaf rows it holds ({@link ENTITY_NAME_SX}).
 */
export const CONTAINER_NAME_SX = {
   fontFamily: MONO_FONT_FAMILY,
   fontSize: FONT_SIZE.body,
   fontWeight: 500,
   color: ink,
} as const;

/** The identifier style for a LEAF row (a field): the thing you came to read. */
export const ENTITY_NAME_SX = {
   fontFamily: MONO_FONT_FAMILY,
   fontSize: FONT_SIZE.small,
   fontWeight: 500,
   color: ink,
} as const;

/**
 * The secondary voice beside a row's name: a type annotation (`string`,
 * `number`) or a dimmed count. Sans on purpose: it describes the row, it isn't
 * a token you would type.
 */
export const ROW_META_SX = {
   fontSize: FONT_SIZE.caption,
   color: "text.secondary",
} as const;
