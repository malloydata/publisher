// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Reading a Malloy result envelope: the rows, and the field metadata that says
// how to present them.
//
// `Publisher.query` already returns plain row objects, which is all most pages
// need. This page calls `Publisher.queryFull` instead, because the envelope also
// carries each field's annotations, and those are where the model states what a
// column is called (`# label`) and how its numbers read (`# currency`,
// `# percent`, `# number`). Reading them means the page keeps no second copy of
// any of that: retag a field in storefront.malloy, reload the package, and the
// tables here change with no edit to this directory.
//
// The tag reading below is a small subset of MOTLY, enough for the tags this
// page acts on, and unlike the filter encoding in format.js it is NOT delegated
// to Malloy's own parser. That is a deliberate line rather than an oversight.
// A filter this page gets wrong returns different ROWS and says nothing; a tag
// it fails to read shows a raw column name or an unformatted number, which the
// reader can see. So the filter grammar is vendored and this is not.

/** Pull the value out of one Malloy cell, whatever kind it is. */
function cellValue(cell) {
   if (!cell || typeof cell !== "object") return null;
   switch (cell.kind) {
      case "string_cell":
         return cell.string_value;
      case "number_cell":
         return cell.number_value;
      case "boolean_cell":
         return cell.boolean_value;
      case "date_cell":
         return cell.date_value;
      case "timestamp_cell":
         return cell.timestamp_value;
      case "null_cell":
         return null;
      default:
         // A nested table (`record_cell` / `array_cell`) is not something this
         // page renders; a tile that needs one should query it as its own tile.
         return null;
   }
}

const annotationTexts = (field) =>
   (field.annotations ?? []).map((a) => String(a.value ?? ""));

/** `# label="Category"` -> `Category`. */
function readLabel(texts) {
   for (const text of texts) {
      const match = /^#\s+label\s*=\s*"([^"]*)"/.exec(text);
      if (match) return match[1];
   }
   return undefined;
}

/**
 * Which formatter a field asks for. `# number=` carries a Malloy/Excel-style
 * pattern, and this page does NOT implement that pattern language: it reads
 * only whether the pattern is `id`, which means leave the digits alone, and
 * treats every other pattern as "a number with one decimal place". That is
 * enough for this model, whose only other pattern is `#,##0.0`. A page needing
 * more should render through `<malloy-render>`, which implements the real
 * thing, rather than growing a second half-copy of it here.
 */
function readFormat(texts, name) {
   for (const text of texts) {
      if (/^#\s+currency\b/.test(text)) return "currency";
      if (/^#\s+percent\b/.test(text)) return "percent";
      const number = /^#\s+number\s*=\s*"?([^"\s]+)"?/.exec(text);
      if (number) return number[1] === "id" ? "id" : "decimal";
   }
   // Not a tag: a month-truncated timestamp is a date the page shows as Jan 2024.
   return name.endsWith("_month") ? "month" : undefined;
}

/**
 * A measure carries `calculation` in Malloy's own annotation. It is how the
 * Console tells a dimension from an aggregate, and the table uses it to decide
 * which columns align right.
 */
const readIsAggregate = (texts) =>
   texts.some((text) => /^#\(malloy\)[^\n]*\bcalculation\b/.test(text));

/**
 * Turn one envelope into `{ fields, rows }`, where each field knows its own
 * heading and format, and each row is a plain object keyed by field name.
 */
export function readResult(envelope) {
   const fields = (envelope?.schema?.fields ?? []).map((field) => {
      const texts = annotationTexts(field);
      return {
         name: field.name,
         label: readLabel(texts) ?? field.name,
         format: readFormat(texts, field.name),
         isAggregate: readIsAggregate(texts),
      };
   });

   const records = envelope?.data?.array_value ?? [];
   const rows = records.map((record) => {
      const cells = record.record_value ?? [];
      const row = {};
      fields.forEach((field, i) => {
         row[field.name] = cellValue(cells[i]);
      });
      return row;
   });

   return { fields, rows };
}
