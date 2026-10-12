// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Writes a package's catalog to `catalog_nodes` and `catalog_edges` (see
 * storage/duckdb/schema.ts) and formats its build report. The rows are a copy
 * for an operator to read; nothing at request time reads them back, so a
 * failed write is logged by the caller and never fails a search.
 */

import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import {
   JOIN_WALK_MAX_HOPS,
   JOIN_WALK_MAX_PATHS,
   type Catalog,
   type CatalogReport,
} from "./catalog";

/** Rows per INSERT statement. */
const INSERT_BATCH = 200;

async function insertBatched(
   db: DuckDBConnection,
   head: string,
   rowPlaceholder: string,
   rows: unknown[][],
): Promise<void> {
   for (let i = 0; i < rows.length; i += INSERT_BATCH) {
      const batch = rows.slice(i, i + INSERT_BATCH);
      await db.run(
         `${head} VALUES ${batch.map(() => rowPlaceholder).join(", ")}`,
         batch.flat(),
      );
   }
}

/** Replace the stored catalog of one package with `catalog`. */
export async function saveCatalog(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
   catalog: Catalog,
   textOf: (nodeKey: string) => string,
): Promise<void> {
   await deleteCatalog(db, environmentName, packageName);
   const now = new Date().toISOString();
   await insertBatched(
      db,
      `INSERT INTO catalog_nodes (environment_name, package_name, node_key, entity_id, kind, source_key, name, model_paths, derived_text, updated_at)`,
      "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [...catalog.nodes.values()].map((n) => [
         environmentName,
         packageName,
         n.key,
         n.id,
         n.kind,
         n.sourceKey,
         n.name,
         JSON.stringify(n.modelPaths),
         textOf(n.key),
         now,
      ]),
   );
   const seen = new Set<string>();
   const edgeRows: unknown[][] = [];
   for (const e of catalog.edges) {
      const path = (e.path ?? []).join(".");
      const id = [e.kind, e.from, e.to, path].join("\u0000");
      if (seen.has(id)) continue;
      seen.add(id);
      edgeRows.push([environmentName, packageName, e.kind, e.from, e.to, path]);
   }
   await insertBatched(
      db,
      `INSERT INTO catalog_edges (environment_name, package_name, kind, from_key, to_key, path)`,
      "(?, ?, ?, ?, ?, ?)",
      edgeRows,
   );
}

export async function deleteCatalog(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<void> {
   for (const table of ["catalog_nodes", "catalog_edges"]) {
      await db.run(
         `DELETE FROM ${table} WHERE environment_name = ? AND package_name = ?`,
         [environmentName, packageName],
      );
   }
}

/** The catalog part of the build report, as one log line. */
export function catalogReportLine(report: CatalogReport): string {
   const nodes = Object.entries(report.nodes)
      .map(([k, v]) => `${v} ${k}`)
      .join(", ");
   const edges = Object.entries(report.edges)
      .map(([k, v]) => `${v} ${k}`)
      .join(", ");
   return (
      `${report.sources} sources; nodes: ${nodes}; edges: ${edges}; ` +
      `${report.multiFileNodes} nodes resolve in more than one file; ` +
      `${report.indexedDimensions} #(index) dimensions; ${report.gatedSources} gated sources; ` +
      `${report.unresolvedReferences} field references resolved to no node; ` +
      `${report.joinPaths} join paths, the longest ${report.deepestJoinPath} joins` +
      (report.joinPathsCapped > 0
         ? `; ${report.joinPathsCapped} join paths stopped at the guard (${JOIN_WALK_MAX_HOPS} joins, or ${JOIN_WALK_MAX_PATHS} paths from one source)`
         : "") +
      (report.joinedCopiesDropped > 0
         ? `; ${report.joinedCopiesDropped} joined copies left out (searched on their own source, reached by route)`
         : "")
   );
}
