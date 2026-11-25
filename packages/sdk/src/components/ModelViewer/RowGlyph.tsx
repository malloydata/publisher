// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import { createElement } from "react";
import {
   IconBoolean,
   IconCalculation,
   IconColumn,
   IconDate,
   IconJoin,
   IconList,
   IconNumber,
   IconRecord,
   IconResult,
   IconText,
   IconTimestamp,
   type Icon,
} from "./icons";
import { ACCENT, kindAccent } from "./tokens";

/**
 * Every kind of row the model page draws, one glyph each: a model's dimensions,
 * measures, calculations, views (and named queries) and joins, and a table's
 * columns, which take a dimension's marks.
 */
export type RowGlyphKind =
   | "dimension"
   | "measure"
   | "calculate"
   | "column"
   | "view"
   | "join";

/**
 * A field kind's glyph when its type says nothing more: a view takes the Result
 * glyph (it opens on its Result); a join, two lines merging; a field with no
 * known type, a column.
 */
const MODEL_GLYPH: Record<
   "dimension" | "measure" | "calculate" | "view" | "join",
   Icon
> = {
   dimension: IconColumn,
   measure: IconNumber,
   calculate: IconCalculation,
   view: IconResult,
   join: IconJoin,
};

/** The hue each kind holds: its field accent (`kindAccent`), a join brown. */
function glyphColor(kind: RowGlyphKind): string {
   switch (kind) {
      case "dimension":
      case "measure":
      case "calculate":
      case "view":
         return kindAccent(kind).main;
      // A table column is what a dimension is drawn from: its hue and marks.
      case "column":
         return kindAccent("dimension").main;
      case "join":
         return ACCENT.brown.main;
   }
}

/**
 * A field's glyph by its DATA TYPE (text, number, date, timestamp, boolean,
 * list, record), the way a BI tool marks its fields: the glyph says what kind
 * of value it is, and the hue says whether it groups or aggregates. `dataType`
 * is the row's own type detail ("number · integer", "date · month",
 * "array<string>"); an unknown or missing type keeps the role's glyph.
 */
export function fieldTypeGlyph(dataType: string | undefined): Icon | undefined {
   const t = dataType?.trim().toLowerCase() ?? "";
   if (!t) return undefined;
   if (t.startsWith("array")) return IconList;
   if (t.startsWith("record") || /\bfields?\b/.test(t)) return IconRecord;
   const base = t.split(/[\s·<]/)[0];
   switch (base) {
      case "string":
         return IconText;
      case "number":
         return IconNumber;
      case "date":
         return IconDate;
      case "timestamp":
      case "timestamptz":
         return IconTimestamp;
      case "boolean":
         return IconBoolean;
      default:
         return undefined;
   }
}

/** One step over the row's 13px name. */
export const ROW_GLYPH_SIZE = 16;

/**
 * The bare glyph that leads a model-page row: 16px, in its kind's hue (a
 * field's glyph by its data type, `fieldTypeGlyph`). Decorative: the section a
 * row sits in already names its kind, and the row's button names the row, so it
 * is hidden from assistive tech.
 */
export function RowGlyph({
   kind,
   dataType,
}: {
   kind: RowGlyphKind;
   dataType?: string;
}) {
   const typed =
      kind === "dimension" ||
      kind === "measure" ||
      kind === "calculate" ||
      kind === "column"
         ? fieldTypeGlyph(dataType)
         : undefined;
   return (
      <Box
         component="span"
         aria-hidden
         data-row-glyph={kind}
         sx={{
            display: "inline-flex",
            alignSelf: "center",
            flexShrink: 0,
            color: glyphColor(kind),
         }}
      >
         {createElement(
            typed ?? MODEL_GLYPH[kind === "column" ? "dimension" : kind],
            { size: ROW_GLYPH_SIZE },
         )}
      </Box>
   );
}
