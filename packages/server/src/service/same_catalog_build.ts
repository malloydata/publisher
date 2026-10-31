// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";

type ApiConnection = components["schemas"]["Connection"];

/**
 * Whether a source's connection and a storage destination are the SAME DuckLake
 * catalog, decided by what locates the catalog, never by name: a connection and
 * a destination may share a name and mean different catalogs (the namespaces are
 * separate on purpose), and two names may reach one catalog. A catalog is its
 * metadata: the catalog database (host, port, database) and the metadata schema
 * in it. The data path is recorded in the catalog, so it is compared only when
 * both configs state one: two configs that state different paths would write
 * the one catalog's files to two places, and are not treated as the same.
 * Only the discrete `host`/`port`/`databaseName` fields are compared: a
 * `connectionString` config never matches, which fails closed (the build is
 * refused as an unsupported source, never run on the wrong catalog).
 */
export function isSameCatalog(
   source: ApiConnection | undefined,
   destination: ApiConnection | undefined,
): boolean {
   const a =
      source?.type === "ducklake" ? source.ducklakeConnection : undefined;
   const b =
      destination?.type === "ducklake"
         ? destination.ducklakeConnection
         : undefined;
   if (!a || !b) return false;
   const pa = a.catalog?.postgresConnection;
   const pb = b.catalog?.postgresConnection;
   if (!pa || !pb) return false;
   const path = (url: string | undefined) => (url ?? "").replace(/\/+$/, "");
   const pathA = path(a.storage?.bucketUrl);
   const pathB = path(b.storage?.bucketUrl);
   return (
      !!pa.host &&
      pa.host === pb.host &&
      (pa.port ?? 5432) === (pb.port ?? 5432) &&
      !!pa.databaseName &&
      pa.databaseName === pb.databaseName &&
      (a.catalog.metadataSchema ?? "") === (b.catalog.metadataSchema ?? "") &&
      (pathA === "" || pathB === "" || pathA === pathB)
   );
}

/** A catalog table path: dot-separated plain identifiers, nothing a reader resolves. */
const CATALOG_TABLE_PATH =
   /^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)*$/;

/** Max IR depth to walk; deep enough for real sources, a hang backstop. */
const MAX_DEPTH = 200;

/**
 * Whether a compiled source definition carries SQL its author wrote, or reads
 * something other than the catalog's own tables: an `.sql()` source
 * (`type: 'sql_select'`), a raw SQL expression (`node: 'genericSQLExpr'`, which
 * `sql_number()` and its siblings compile to), or a table whose path is a file
 * or URL rather than a dotted identifier, anywhere beneath it, joins and
 * query-source pipelines included.
 *
 * A same-catalog build runs on a READ-WRITE session on the catalog, so it may run
 * only SQL Malloy generated; this is what keeps author text off that session.
 *
 * A built-in function call (`round`, `nullif`, a `.day` truncation's helper) carries
 * its overload's definition in the IR, and that definition's per-dialect template is a
 * `genericSQLExpr`: the compiler's SQL, not the author's. The walk skips a call's
 * `overload` and keeps walking its arguments, where an author's `sql_number()` would
 * be. Without that, any source using a built-in, a pre-aggregate rollup over one
 * included, read as author SQL.
 * Fails closed: an unreadable definition, or one deeper than the walk allows,
 * counts as carrying author SQL.
 */
export function carriesAuthorSql(def: unknown): boolean {
   const seen = new Set<object>();
   const walk = (node: unknown, depth: number): boolean => {
      if (depth > MAX_DEPTH) return true;
      if (node === null || typeof node !== "object") return false;
      if (seen.has(node)) return false;
      seen.add(node);
      if (Array.isArray(node)) {
         return node.some((child) => walk(child, depth + 1));
      }
      const record = node as Record<string, unknown>;
      if (record.type === "sql_select" || record.node === "genericSQLExpr") {
         return true;
      }
      if (record.node === "function_call") {
         return Object.entries(record).some(
            ([key, child]) => key !== "overload" && walk(child, depth + 1),
         );
      }
      // DuckDB reads a file or URL named as a table (`lake.table('/x.csv')`,
      // `lake.table('s3://other/y.parquet')`), which on this session would run
      // with the destination's storage credential: only a catalog table, a
      // dotted identifier path, is Malloy reading the catalog.
      if (
         record.type === "table" &&
         typeof record.tablePath === "string" &&
         !CATALOG_TABLE_PATH.test(record.tablePath)
      ) {
         return true;
      }
      return Object.values(record).some((child) => walk(child, depth + 1));
   };
   try {
      if (def === null || typeof def !== "object") return true;
      return walk(def, 0);
   } catch {
      return true;
   }
}
