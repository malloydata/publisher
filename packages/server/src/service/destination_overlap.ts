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
 * catalog database and metadata schema and by its data path. A DuckDB
 * connection that attaches the destination through `setupSQL`, or a
 * connection that reads its data files directly, is not detected.
 *
 * Pure.
 */

import type { components } from "../api";

type ApiConnection = components["schemas"]["Connection"];
type PostgresConnection = components["schemas"]["PostgresConnection"];

/** The catalog database a DuckLake config names, normalized. */
function catalogKey(pg: PostgresConnection | undefined): string | undefined {
   if (!pg) return undefined;
   if (pg.connectionString?.trim()) return pg.connectionString.trim();
   if (!pg.databaseName) return undefined;
   return `${(pg.host ?? "localhost").toLowerCase()}:${pg.port ?? 5432}/${pg.databaseName}`;
}

/** A data path, normalized for a prefix comparison. */
function dataPath(url: string | undefined): string | undefined {
   const trimmed = url?.trim().replace(/\/+$/, "");
   return trimmed ? `${trimmed}/` : undefined;
}

/**
 * Why `connection` reaches `destination`'s tables, or undefined when this
 * check finds no overlap.
 */
export function destinationOverlap(
   connection: ApiConnection,
   destination: ApiConnection,
): string | undefined {
   if (connection.type !== "ducklake" || destination.type !== "ducklake") {
      return undefined;
   }
   const c = connection.ducklakeConnection;
   const d = destination.ducklakeConnection;
   if (!c || !d) return undefined;
   const cCatalog = catalogKey(c.catalog?.postgresConnection);
   if (
      cCatalog !== undefined &&
      cCatalog === catalogKey(d.catalog?.postgresConnection) &&
      (c.catalog?.metadataSchema ?? "") === (d.catalog?.metadataSchema ?? "")
   ) {
      return "it names the same DuckLake catalog";
   }
   const cPath = dataPath(c.storage?.bucketUrl);
   const dPath = dataPath(d.storage?.bucketUrl);
   if (cPath && dPath && (cPath.startsWith(dPath) || dPath.startsWith(cPath))) {
      return "its data path overlaps the destination's";
   }
   return undefined;
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
