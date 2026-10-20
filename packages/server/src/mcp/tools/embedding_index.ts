// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the rest of the server shares about an entity's identity in the search
 * index, and the clean-up of the rows an earlier release wrote. The index
 * itself is the find, list, judge engine's (../context/).
 */

import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import {
   deleteEnvironmentSourceSummaries,
   deletePackageSourceSummaries,
} from "./source_summaries";

/** The subset of the tool's Entity shape the index needs. */
export interface EmbeddableEntity {
   kind: string;
   name: string;
   source: string | undefined;
   modelPath: string;
   // #(doc)-only text (see get_context_tool docOnlyText). Deliberately NOT
   // the response `doc`, which can fall back to raw annotation lines and
   // would leak #(authorize) predicates to the embedding provider.
   embedDoc: string;
   /** Malloy type of a dimension or measure; absent on other kinds. */
   dataType?: string;
   /** Authored expression text; sent to the LLM only under the `full` egress preset. */
   code?: string;
   /** Join cardinality, on `kind: "join"` entities only. */
   relationship?: string;
   /**
    * On a `join`: the source it joins to, when the compiled model names one.
    * Read by the source summary to nest a joined source's fields.
    */
   joinTarget?: string;
   /** The file that defines `joinTarget`: the name alone is not an identity. */
   joinTargetModelPath?: string;
   /** On a joined copy of a field: the join path that reaches it (`buyer.origin`). */
   joinPath?: string;
}

/**
 * Turn an identifier into the words a person would search with:
 * `dep_delay` -> "dep delay", `totalSales` -> "total sales".
 */
export function humanizeName(name: string): string {
   return name
      .replace(/[_\-./]+/g, " ")
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trim();
}

/**
 * Separator for the composite in-memory keys below. NUL, not `|`: a
 * backtick-quoted Malloy identifier may contain `|`, so two entities could
 * collide on one key and the sync diff would silently skip a delete or a
 * re-embed. A control character cannot appear in an identifier.
 */
export const KEY_SEPARATOR = "\u0000";

/**
 * The stable key for one entity, shared by the index dedup, row identity and
 * the tool's result mapping. Pass "" for a sourceless entity. In-memory only:
 * the database keys on the three columns directly.
 */
export function entityRowKey(
   kind: string,
   source: string,
   name: string,
): string {
   return [kind, source, name].join(KEY_SEPARATOR);
}

/**
 * What a package's search index is currently doing, as the REST Package
 * resource reports it (`embeddingIndex`). The row and entity counts are
 * always zero: the engine keeps no per-entity index to count, and the REST
 * contract still carries them.
 */
export interface EmbeddingIndexStatus {
   /**
    * - `lexical`: no embedding provider is configured. A mode, not a fallback.
    * - `indexing`: the index is being built (or is queued to be).
    * - `ready`: the next question is answered from the built index.
    * - `error`: the index cannot serve now; `lastError` says why.
    */
   status: "lexical" | "indexing" | "ready" | "error";
   lastError?: { message: string; retryAt?: string };
   totalRows: number;
   embeddedRows: number;
   totalEntities: number;
   embeddedEntities: number;
   /** The engine's own state: its build status and, once built, its build report. */
   fcj?: Record<string, unknown>;
}

/**
 * Remove a deleted package's rows from the tables an earlier release's index
 * wrote (cached embeddings and keyphrases), and its source summaries. Nothing
 * reads or writes the first two now; the delete keeps a long-lived database
 * from holding them for a package that is gone. Best-effort: a missed cleanup
 * is inert, because every read is scoped by environment and package.
 */
export async function deletePackageEmbeddings(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<void> {
   await db.run(
      `DELETE FROM entity_embeddings
       WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
   await db.run(
      `DELETE FROM entity_keyphrases
       WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
   await deletePackageSourceSummaries(db, environmentName, packageName);
}

/** Remove a deleted environment's rows from the same tables. */
export async function deleteEnvironmentEmbeddings(
   db: DuckDBConnection,
   environmentName: string,
): Promise<void> {
   await db.run(`DELETE FROM entity_embeddings WHERE environment_name = ?`, [
      environmentName,
   ]);
   await db.run(`DELETE FROM entity_keyphrases WHERE environment_name = ?`, [
      environmentName,
   ]);
   await deleteEnvironmentSourceSummaries(db, environmentName);
}
