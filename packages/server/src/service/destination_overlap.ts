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
 * it (docs/security-posture.md).
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
import { connectionDialectOf } from "./dialects";

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

/** Every connection that reaches a destination, with why. */
export function destinationOverlaps(
   connections: readonly ApiConnection[],
   destinations: readonly ApiConnection[],
): { connection: string; destination: string; reason: string }[] {
   const out: { connection: string; destination: string; reason: string }[] =
      [];
   for (const connection of connections) {
      for (const destination of destinations) {
         const reason = destinationOverlap(connection, destination);
         if (reason) {
            out.push({
               connection: connection.name ?? "",
               destination: destination.name ?? "",
               reason,
            });
         }
      }
   }
   return out;
}
