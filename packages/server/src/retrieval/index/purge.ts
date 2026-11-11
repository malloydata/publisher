// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The clean-up a delete owes: every row retrieval stored for a package or an
 * environment. Its own module so the entity-identity helpers in
 * embedding_index.ts need not import the stores they are keys into.
 */

import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { deleteEngineRows } from "./catalog_store";
import {
   deleteEnvironmentSourceSummaries,
   deletePackageSourceSummaries,
} from "./source_summaries";

/**
 * Remove a deleted package's rows: the engine's tables (its catalog, LLM
 * output, vectors and stored values), the tables an earlier release's index
 * wrote (cached embeddings and keyphrases, which nothing reads or writes
 * now), and its source summaries. The stored values are warehouse data, so
 * they must not outlive the package.
 */
export async function deletePackageEmbeddings(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<void> {
   // First: these hold warehouse values, the rest only model text.
   await deleteEngineRows(db, environmentName, packageName);
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
   await deleteEngineRows(db, environmentName);
   await db.run(`DELETE FROM entity_embeddings WHERE environment_name = ?`, [
      environmentName,
   ]);
   await db.run(`DELETE FROM entity_keyphrases WHERE environment_name = ?`, [
      environmentName,
   ]);
   await deleteEnvironmentSourceSummaries(db, environmentName);
}
