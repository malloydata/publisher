// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether an environment connection reaches a storage destination's tables.
 *
 * A destination holds stored tables under readable names, gated sources'
 * included (every caller's rows, ungated, the gate re-applied only when
 * Publisher reads them), and models cannot name a destination. A connection
 * a model can name that targets the same DuckLake (its catalog database and
 * metadata schema, or its data path) can, so any package could read another
 * package's gated rows straight from the table. Publisher cannot stop that
 * configuration; it warns at environment load, and the docs say never to do
 * it (docs/security-posture.md). Only a `readableSchemas` entry naming the
 * destination's schema is refused ({@link refuseListedDestinationSchema}).
 *
 * Cheap and conservative: only a DuckLake connection is compared, by its
 * catalog database and metadata schema and by its data path (the
 * comparison is the registry entry's, dialects/ducklake.ts). A DuckDB
 * connection that attaches the destination through `setupSQL`, or a
 * connection that reads its data files directly, is not detected.
 *
 * Pure.
 */

import type { components } from "../api";
import { BadRequestError } from "../errors";
import { managedSchemas, normalizeReadableSchemas } from "./connection_type";
import { connectionDialectOf } from "./dialects";
import { destinationSchemaOf } from "./materialization/domain/storage_table_path";

type ApiConnection = components["schemas"]["Connection"];

/**
 * Why `connection` reaches `destination`'s tables, or undefined when this
 * check finds no overlap. Only two connections of one type whose registry
 * entry can compare them (DuckLake's) are compared.
 */
export function destinationOverlap(
   connection: ApiConnection,
   destination: ApiConnection,
): string | undefined {
   const overlap = connectionDialectOf(destination.type)?.destination?.overlap;
   if (overlap === undefined || connection.type !== destination.type) {
      return undefined;
   }
   return overlap(connection, destination);
}

/** Every connection that reaches a destination, with why, the destination's build schema, and whether the connection can read that schema. */
export function destinationOverlaps(
   connections: readonly ApiConnection[],
   destinations: readonly ApiConnection[],
): {
   connection: string;
   destination: string;
   schema: string;
   reason: string;
   readable: boolean;
   listsDestinationSchema?: string;
}[] {
   const out: ReturnType<typeof destinationOverlaps> = [];
   for (const connection of connections) {
      for (const destination of destinations) {
         const reason = destinationOverlap(connection, destination);
         if (reason) {
            const schema = destinationSchemaOf(destination);
            out.push({
               connection: connection.name ?? "",
               destination: destination.name ?? "",
               schema,
               reason,
               // A managed read-only view's credential and schema list stop at `main` and its readableSchemas.
               readable:
                  !(
                     connection.managed === true &&
                     connection.ducklakeConnection?.readOnly !== false
                  ) ||
                  managedSchemas(
                     connection.ducklakeConnection?.readableSchemas,
                  ).has(schema.toLowerCase()),
               // `listsDestinationSchema` makes the schema's stored copies readable by name; DuckDB matches schema names regardless of case.
               ...(normalizeReadableSchemas(
                  connection.ducklakeConnection?.readableSchemas,
               )?.some((name) => name.toLowerCase() === schema.toLowerCase())
                  ? { listsDestinationSchema: schema }
                  : {}),
            });
         }
      }
   }
   return out;
}

/** Refuses a connection whose `readableSchemas` lists a schema a destination on its catalog builds into: its stored copies, gated sources' rows among them, would be readable by name without their gate. */
export function refuseListedDestinationSchema(
   connections: readonly ApiConnection[],
   destinations: readonly ApiConnection[],
): void {
   const listed = destinationOverlaps(connections, destinations).find(
      (overlap) => overlap.listsDestinationSchema,
   );
   if (listed) {
      throw new BadRequestError(
         `Connection "${listed.connection}" lists schema "${listed.listsDestinationSchema}" in readableSchemas, which storage destination "${listed.destination}" builds into: that destination's stored copies, gated sources' rows among them, would be readable by name without their gate. Remove it from readableSchemas, or set the destination's destinationSchema to another schema.`,
      );
   }
}
