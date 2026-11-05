// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";

/**
 * The rows behind a value as a CSV file: what the rows dialog's default
 * download writes. A `select: *` result is one flat table, so each schema
 * field is a column and each record a line.
 */

/** A text value a spreadsheet would run as a formula is written with a leading `'`. */
export function neutralizeFormula(value: string): string {
   return /^[=+\-@\t\r]/.test(value) ? `'${value}` : value;
}

function quote(value: string): string {
   return /[",\r\n]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
}

/**
 * One cell as a CSV field. Text (strings, JSON, a database-native value, a
 * nested record or array as JSON) is neutralized and quoted; a number, a
 * boolean, a date and a timestamp are written as they are, so a negative
 * number stays a number. A null is an empty field.
 */
function csvField(cell: Malloy.Cell | undefined): string {
   if (cell === undefined) return "";
   switch (cell.kind) {
      case "null_cell":
         return "";
      case "number_cell":
         return cell.string_value ?? String(cell.number_value);
      case "boolean_cell":
         return cell.boolean_value ? "true" : "false";
      case "date_cell":
         return cell.date_value;
      case "timestamp_cell":
         return cell.timestamp_value;
      case "string_cell":
         return quote(neutralizeFormula(cell.string_value));
      case "json_cell":
         return quote(neutralizeFormula(cell.json_value));
      case "sql_native_cell":
         return quote(neutralizeFormula(cell.sql_native_value));
      default:
         return quote(neutralizeFormula(JSON.stringify(cell)));
   }
}

/** The records of a result, in order. */
export function resultRecords(result: Malloy.Result): Malloy.Cell[][] {
   const data = result.data;
   if (data?.kind !== "array_cell") return [];
   return data.array_value.flatMap((row) =>
      row.kind === "record_cell" ? [row.record_value] : [],
   );
}

/**
 * The result as CSV text: a header of the field names, then one line per
 * record. Starts with a byte-order mark and ends lines with CRLF, so a
 * spreadsheet opens non-ASCII text and line breaks inside a value correctly.
 */
export function resultToCsv(result: Malloy.Result): string {
   const fields = result.schema.fields;
   const lines = [
      // A field name is text a spreadsheet would run too.
      fields.map((f) => quote(neutralizeFormula(f.name))).join(","),
      ...resultRecords(result).map((record) =>
         fields.map((_, i) => csvField(record[i])).join(","),
      ),
   ];
   return `\uFEFF${lines.join("\r\n")}\r\n`;
}

/**
 * A file name for the rows behind a value, without an extension: the source
 * and each value the rows are filtered on, as the dialog's title names them.
 */
export function rowsFileName(
   source: string,
   values: readonly string[],
): string {
   const name = [source, "rows", ...values]
      .join("_")
      .replace(/[^A-Za-z0-9._-]+/g, "_")
      .replace(/_+/g, "_")
      .replace(/^[_.]+|[_.]+$/g, "");
   return name.slice(0, 120) || "rows";
}

/**
 * How many rows the server's own cap left in a result, when that cap (not
 * the query) cut it short; undefined when the result is complete.
 */
export function serverCappedAt(
   rowCount: number,
   queryRowLimit: number | undefined,
   queryRowLimitSource: string | undefined,
): number | undefined {
   return queryRowLimitSource === "server_default" &&
      queryRowLimit !== undefined &&
      rowCount >= queryRowLimit
      ? queryRowLimit
      : undefined;
}
