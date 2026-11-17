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
 * Pure, but for resolving a local data path through its symlinks.
 */

import * as fs from "fs";
import * as path from "path";
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

/** Loopback spellings, which name one host. */
const LOOPBACK: ReadonlySet<string> = new Set([
   "localhost",
   "127.0.0.1",
   "::1",
   "[::1]",
]);

/**
 * A catalog database in one canonical spelling (`host:port/database`, the
 * host lower-cased and loopback folded), and the form it was written in, or
 * undefined when it cannot be read.
 */
function canonicalCatalog(
   pg: PostgresConnection | undefined,
): { key: string; form: "url" | "fields" } | undefined {
   if (!pg) return undefined;
   const host = (h: string | undefined) => {
      const lower = (h ?? "localhost").toLowerCase();
      return LOOPBACK.has(lower) ? "localhost" : lower;
   };
   const url = pg.connectionString?.trim();
   if (url) {
      try {
         const parsed = new URL(url);
         const database = decodeURIComponent(
            parsed.pathname.replace(/^\//, ""),
         );
         if (!database) return undefined;
         return {
            key: `${host(parsed.hostname)}:${parsed.port || "5432"}/${database}`,
            form: "url",
         };
      } catch {
         return undefined;
      }
   }
   if (!pg.databaseName) return undefined;
   return {
      key: `${host(pg.host)}:${pg.port ?? 5432}/${pg.databaseName}`,
      form: "fields",
   };
}

/**
 * A data path in one canonical spelling, ending `/`: a URL's scheme and
 * bucket lower-cased and its path's `.` and `..` segments collapsed; a local
 * path (or `file://`) resolved, and through its symlinks where it exists.
 */
function canonicalDataPath(url: string | undefined): string | undefined {
   const trimmed = url?.trim();
   if (!trimmed) return undefined;
   const scheme = /^([A-Za-z][A-Za-z0-9+.-]*):\/\/(.*)$/.exec(trimmed);
   let canonical: string;
   if (scheme && scheme[1].toLowerCase() !== "file") {
      const rest = scheme[2];
      const slash = rest.indexOf("/");
      const bucket = (slash < 0 ? rest : rest.slice(0, slash)).toLowerCase();
      const key = path.posix.normalize(
         `/${slash < 0 ? "" : rest.slice(slash + 1)}`,
      );
      canonical = `${scheme[1].toLowerCase()}://${bucket}${key}`;
   } else {
      let local = path.resolve(scheme ? scheme[2] : trimmed);
      try {
         local = fs.realpathSync(local);
      } catch {
         // Not there (yet): resolved without its symlinks.
      }
      canonical = local;
   }
   return `${canonical.replace(/\/+$/, "")}/`;
}

/**
 * Whether two DuckLake configs provably hold different tables under one
 * name. Only two ways: one catalog (database and metadata schema, compared
 * in one canonical spelling) with different table schemas, for a name with
 * no container of its own (a name with one is placed there, whatever the
 * schema); or different catalogs, written in the same form, whose data
 * paths are both known and do not overlap. Anything else may be one DuckLake
 * reached twice (a destination renamed, or two names for one lake), so it is
 * not proven. `schemaOf` gives the schema each builds into.
 */
function ducklakesApart(
   a: ApiConnection,
   b: ApiConnection,
   schemaOf: (c: ApiConnection) => string,
   qualified: boolean,
): boolean {
   if (a.type !== "ducklake" || b.type !== "ducklake") return false;
   const x = a.ducklakeConnection;
   const y = b.ducklakeConnection;
   if (!x || !y) return false;
   const xCatalog = canonicalCatalog(x.catalog?.postgresConnection);
   const yCatalog = canonicalCatalog(y.catalog?.postgresConnection);
   if (xCatalog === undefined || yCatalog === undefined) return false;
   const sameCatalog =
      xCatalog.key === yCatalog.key &&
      (x.catalog?.metadataSchema ?? "").toLowerCase() ===
         (y.catalog?.metadataSchema ?? "").toLowerCase();
   if (sameCatalog) {
      return (
         !qualified && schemaOf(a).toLowerCase() !== schemaOf(b).toLowerCase()
      );
   }
   if (xCatalog.form !== yCatalog.form) return false;
   const xPath = canonicalDataPath(x.storage?.bucketUrl);
   const yPath = canonicalDataPath(y.storage?.bucketUrl);
   return (
      !!xPath && !!yPath && !xPath.startsWith(yPath) && !yPath.startsWith(xPath)
   );
}

/**
 * Whether two storage destinations provably hold different tables under
 * `name` (see ducklakesApart), each in the schema it builds into.
 */
export function destinationsApart(
   a: ApiConnection,
   b: ApiConnection,
   name: string,
): boolean {
   return ducklakesApart(a, b, destinationSchemaOf, name.includes("."));
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
   name: string,
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
      return ducklakesApart(
         connection,
         destination,
         (c) => (c === connection ? "main" : destinationSchemaOf(c)),
         name.includes("."),
      );
   }
   return false;
}
