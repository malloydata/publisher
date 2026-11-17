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
import { destinationSchemaOf } from "./materialization/domain/storage_table_path";

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

/**
 * Whether two DuckLake configs provably hold different tables under one
 * name: the same catalog (database and metadata schema) with different
 * table schemas, or different catalogs whose data paths are both known and
 * do not overlap. Anything else may be one DuckLake reached twice (a
 * destination renamed, or two names for one lake), so it is not proven.
 * `schemaOf` gives the schema each builds its tables into.
 */
function ducklakesApart(
   a: ApiConnection,
   b: ApiConnection,
   schemaOf: (c: ApiConnection) => string,
): boolean {
   if (a.type !== "ducklake" || b.type !== "ducklake") return false;
   const x = a.ducklakeConnection;
   const y = b.ducklakeConnection;
   if (!x || !y) return false;
   const xCatalog = catalogKey(x.catalog?.postgresConnection);
   const yCatalog = catalogKey(y.catalog?.postgresConnection);
   if (xCatalog === undefined || yCatalog === undefined) return false;
   const sameCatalog =
      xCatalog === yCatalog &&
      (x.catalog?.metadataSchema ?? "") === (y.catalog?.metadataSchema ?? "");
   if (sameCatalog) return schemaOf(a) !== schemaOf(b);
   const xPath = dataPath(x.storage?.bucketUrl);
   const yPath = dataPath(y.storage?.bucketUrl);
   return (
      !!xPath && !!yPath && !xPath.startsWith(yPath) && !yPath.startsWith(xPath)
   );
}

/**
 * Whether two storage destinations provably hold different tables under one
 * name (see ducklakesApart), each in the schema it builds into.
 */
export function destinationsApart(a: ApiConnection, b: ApiConnection): boolean {
   return ducklakesApart(a, b, destinationSchemaOf);
}

/** Connection types that cannot reach a DuckLake table by its name. */
const NEVER_REACH_A_DUCKLAKE: ReadonlySet<string> = new Set([
   "postgres",
   "bigquery",
   "snowflake",
   "trino",
   "databricks",
   "mysql",
   "sqlserver",
]);

/**
 * Whether a table on `connection`, in its own database, provably is not the
 * table of the same name in `destination`. A warehouse that cannot attach a
 * DuckLake never reaches it; a DuckDB connection does only through its
 * setup SQL or attached databases; a DuckLake connection does unless it is
 * provably another lake. MotherDuck, a Publisher proxy, and anything else
 * are not proven.
 */
export function connectionApartFromDestination(
   connection: ApiConnection,
   destination: ApiConnection,
): boolean {
   const type = connection.type ?? "";
   if (NEVER_REACH_A_DUCKLAKE.has(type)) return true;
   if (type === "duckdb") {
      const duckdb = connection.duckdbConnection;
      return (
         !duckdb?.setupSQL?.trim() &&
         (duckdb?.attachedDatabases ?? []).length === 0
      );
   }
   if (type === "ducklake") {
      // A connection's tables are in the lake's `main`.
      return ducklakesApart(connection, destination, (c) =>
         c === connection ? "main" : destinationSchemaOf(c),
      );
   }
   return false;
}
