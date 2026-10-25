// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";

type ApiConnection = components["schemas"]["Connection"];

/**
 * Whether a source's connection and a storage destination are the SAME DuckLake
 * catalog, decided by what locates the catalog, never by name: a connection and
 * a destination may share a name and mean different catalogs (the namespaces are
 * separate on purpose), and two names may reach one catalog. Equal when the
 * catalog database (host, port, database), its metadata schema and its data path
 * all match.
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
   return (
      !!pa.host &&
      pa.host === pb.host &&
      (pa.port ?? 5432) === (pb.port ?? 5432) &&
      !!pa.databaseName &&
      pa.databaseName === pb.databaseName &&
      (a.catalog.metadataSchema ?? "") === (b.catalog.metadataSchema ?? "") &&
      path(a.storage?.bucketUrl) !== "" &&
      path(a.storage?.bucketUrl) === path(b.storage?.bucketUrl)
   );
}

/** Max IR depth to walk; deep enough for real sources, a hang backstop. */
const MAX_DEPTH = 200;

/**
 * Whether a compiled source definition carries SQL its author wrote: an
 * `.sql()` source (`type: 'sql_select'`) or a raw SQL expression
 * (`node: 'genericSQLExpr'`, which `sql_number()` and its siblings compile to),
 * anywhere beneath it, joins and query-source pipelines included.
 *
 * A same-catalog build runs on a READ-WRITE session on the catalog, so it may run
 * only SQL Malloy generated; this is what keeps author text off that session.
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
      return Object.values(record).some((child) => walk(child, depth + 1));
   };
   try {
      if (def === null || typeof def !== "object") return true;
      return walk(def, 0);
   } catch {
      return true;
   }
}
