// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A column type, as INFORMATION_SCHEMA spells it, that holds a record: DuckDB
 * `STRUCT(...)` and `STRUCT(...)[]`, Trino `row(...)` and `array(row(...))`,
 * and a Databricks `STRUCT`. Databricks names an array of structs only
 * `ARRAY` in `data_type` (the element type is in `full_data_type`, which is
 * not read), so its repeated records keep the flat list. A plain array has
 * no members, so it is not read.
 */
export const RECORD_COLUMN_TYPE =
   /^(struct|row)\b|^array\s*[(<]\s*(struct|row)\b/i;

/**
 * Whether a listed column can have members listed after it: a record as
 * INFORMATION_SCHEMA spells it (RECORD_COLUMN_TYPE), or as the Malloy schema
 * does, `record`, or `array` for a BigQuery repeated record.
 */
export function isRecordColumnType(type: string | undefined): boolean {
   return (
      type === "record" ||
      type === "array" ||
      RECORD_COLUMN_TYPE.test(type ?? "")
   );
}
