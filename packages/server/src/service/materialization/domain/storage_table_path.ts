// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a stored table lives in a storage destination: the destination's
 * schema and the table's unquoted path in it.
 *
 * Read by the `storage=` build, the reclaimer and every serving leg that
 * reads a stored table, so a table is named the same way when it is written,
 * read and dropped. A physical name that already carries a schema is kept as
 * the author wrote it.
 *
 * Pure.
 */

import { components } from "../../../api";

type ApiConnection = components["schemas"]["Connection"];

/** DuckLake's default schema, and every destination's schema until one is declared. */
export const DEFAULT_DESTINATION_SCHEMA = "main";

/**
 * The schema a storage destination builds into and binds from: its declared
 * `ducklakeConnection.destinationSchema`, else `main`. An absent value is the
 * pre-existing behavior, so a destination that declares none is unchanged.
 */
export function destinationSchemaOf(
   destination: ApiConnection | undefined,
): string {
   const declared = destination?.ducklakeConnection?.destinationSchema;
   return declared && declared.trim() !== ""
      ? declared
      : DEFAULT_DESTINATION_SCHEMA;
}

/**
 * The unquoted path of a stored table: `<destination>.<physical>`, or
 * `<destination>.<schema>.<physical>` when the destination declares a schema
 * other than `main`. A physical name that already carries a schema (a
 * `#@ persist name="analytics.daily"`) is the author's choice and is kept as
 * written, so the destination's schema never produces a four-part path.
 *
 * The build, the drop, the incremental probe and the serve binding all render
 * the path through this one function: a build and a binding that disagree on
 * the schema 400 every bound query at run time.
 */
export function storageTablePath(
   destinationName: string,
   physicalTableName: string,
   schema: string = DEFAULT_DESTINATION_SCHEMA,
): string {
   if (
      physicalTableName.includes(".") ||
      !schema ||
      schema === DEFAULT_DESTINATION_SCHEMA
   ) {
      return `${destinationName}.${physicalTableName}`;
   }
   return `${destinationName}.${schema}.${physicalTableName}`;
}
