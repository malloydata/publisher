// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rehydrates compact JSON rows + Malloy schema into a full Malloy Result
 * that @malloydata/render can consume.
 *
 * Compact format: flat objects like { region: "California", user_count: 348 }
 * Full format: typed cells like { kind: "record_cell", record_value: [...] }
 */
import type {
   Annotation,
   AtomicType,
   Cell,
   Data,
   DimensionInfo,
   FieldInfo,
   Result,
   Schema,
} from "@malloydata/malloy-interfaces";

/** Metadata passed alongside compact rows to enable rehydration */
export interface ResultMeta {
   schema: Schema;
   annotations?: Annotation[];
   connection_name: string;
   sql?: string;
   model_annotations?: Annotation[];
   query_timezone?: string;
   source_annotations?: Annotation[];
}

/**
 * Reconstruct a full Malloy Result from compact rows and metadata.
 */
export function rehydrate(
   rows: Record<string, unknown>[],
   meta: ResultMeta,
): Result {
   return {
      schema: meta.schema,
      annotations: meta.annotations,
      connection_name: meta.connection_name,
      sql: meta.sql,
      model_annotations: meta.model_annotations,
      query_timezone: meta.query_timezone,
      source_annotations: meta.source_annotations,
      data: rehydrateTopLevel(rows, meta.schema.fields),
   };
}

/**
 * The top-level data is always an array_cell wrapping the rows.
 */
function rehydrateTopLevel(
   rows: Record<string, unknown>[],
   fields: FieldInfo[],
): Data {
   return {
      kind: "array_cell",
      array_value: rows.map((row) => rehydrateRecord(row, fields)),
   };
}

/**
 * Convert a compact row object into a record_cell.
 * Each field in the schema maps to a positional cell in record_value.
 */
function rehydrateRecord(
   row: Record<string, unknown>,
   fields: FieldInfo[],
): Cell {
   return {
      kind: "record_cell",
      record_value: fields.map((field) => {
         const fieldName = getFieldName(field);
         const value = row[fieldName];
         return rehydrateValue(value, getFieldType(field));
      }),
   };
}

/**
 * Coerce a boolean cell, tolerating the string forms a driver may send.
 *
 * `Boolean(value)` was wrong here, and wrong in the direction that renders a
 * chart which is confidently incorrect rather than absent. The `number_type`
 * branch above coerces with `Number(value)` precisely because a driver may hand
 * back a number as text; under that same premise a driver hands back a boolean
 * as text, and `Boolean("false")` is TRUE. So were `Boolean("f")` and
 * `Boolean("0")`. Every false value arriving as a string read as true, and a
 * filter or a grouped chart over it would have been silently inverted.
 *
 * A real boolean passes through. Anything else falls back to truthiness, so an
 * unrecognised shape behaves as it did before rather than becoming false.
 */
const FALSE_STRINGS = new Set(["false", "f", "0", "n", "no", ""]);

function coerceBoolean(value: unknown): boolean {
   if (typeof value === "boolean") return value;
   if (typeof value === "number") return value !== 0;
   if (typeof value === "string") {
      return !FALSE_STRINGS.has(value.trim().toLowerCase());
   }
   return Boolean(value);
}

/**
 * Convert a single compact value into a typed Cell based on the field's type.
 */
function rehydrateValue(value: unknown, type: AtomicType): Cell {
   if (value === null || value === undefined) {
      return { kind: "null_cell" } as Cell;
   }

   switch (type.kind) {
      case "string_type":
         return {
            kind: "string_cell",
            string_value: String(value),
         };

      case "number_type": {
         const numVal = typeof value === "number" ? value : Number(value);
         const cell: Record<string, unknown> = {
            kind: "number_cell",
            number_value: numVal,
         };
         if ("subtype" in type && type.subtype) {
            cell.subtype = type.subtype;
         }
         // For bigint subtype, include string_value for precision
         if ("subtype" in type && type.subtype === "bigint") {
            cell.string_value = String(value);
         }
         return cell as unknown as Cell;
      }

      case "boolean_type":
         return {
            kind: "boolean_cell",
            boolean_value: coerceBoolean(value),
         };

      case "date_type":
         return {
            kind: "date_cell",
            date_value: String(value),
         };

      case "timestamp_type":
      case "timestamptz_type":
         return {
            kind: "timestamp_cell",
            timestamp_value: String(value),
         };

      case "json_type":
         return {
            kind: "json_cell",
            json_value:
               typeof value === "string" ? value : JSON.stringify(value),
         };

      case "sql_native_type":
         return {
            kind: "sql_native_cell",
            sql_native_value: String(value),
         };

      case "array_type": {
         // Nested query results: value is an array of compact sub-rows
         const subRows = Array.isArray(value) ? value : [];
         const elementType = type.element_type;
         if (elementType.kind === "record_type") {
            const subFields = elementType.fields;
            return {
               kind: "array_cell",
               array_value: subRows.map((subRow: Record<string, unknown>) =>
                  rehydrateRecordFromDimensionFields(subRow, subFields),
               ),
            };
         }
         // Scalar array (rare): wrap each element
         return {
            kind: "array_cell",
            array_value: subRows.map((item: unknown) =>
               rehydrateValue(item, elementType),
            ),
         };
      }

      case "record_type": {
         // Inline record: value is a compact object
         const recordObj =
            typeof value === "object" && value !== null
               ? (value as Record<string, unknown>)
               : {};
         return rehydrateRecordFromDimensionFields(recordObj, type.fields);
      }

      default:
         return { kind: "null_cell" } as Cell;
   }
}

/**
 * Rehydrate a compact row using DimensionInfo fields (used in nested record/array types).
 */
function rehydrateRecordFromDimensionFields(
   row: Record<string, unknown>,
   fields: DimensionInfo[],
): Cell {
   return {
      kind: "record_cell",
      record_value: fields.map((dimField) => {
         const value = row[dimField.name];
         return rehydrateValue(value, dimField.type);
      }),
   };
}

/** Extract field name from a FieldInfo union */
function getFieldName(field: FieldInfo): string {
   return (field as { name: string }).name;
}

/** Extract the AtomicType from a FieldInfo union */
function getFieldType(field: FieldInfo): AtomicType {
   return (field as { type: AtomicType }).type;
}
