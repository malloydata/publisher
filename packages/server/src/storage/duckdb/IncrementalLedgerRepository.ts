// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   IncrementalLedgerEntry,
   IncrementalStrategy,
} from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

/**
 * DuckDB-backed store for the incremental `covered_through` boundaries.
 *
 * One row per (environment, connection, physical table): the exclusive upper end
 * of the watermark range that table is known to contain, plus the declarations it
 * was computed under and the source address it was computed for.
 *
 * Keyed on the table because that is what the boundary is a fact about. The source
 * address and the package name are recorded but NOT keys — see the schema comment
 * for why keying on them broke a table shared across package versions.
 *
 * Deliberately has NO locking of its own. The single writer is already
 * guaranteed one level up, by the `active_key` unique index on
 * `materializations` that permits at most one active run per
 * (environment, package) — a lease here could only ever disagree with it.
 *
 * Writes are upserts because a boundary has exactly one current value; there is
 * no history to append. An advance that is applied twice (a retried run) must
 * land on the same row, which is what makes the retry a no-op rather than a
 * second boundary.
 */
export class IncrementalLedgerRepository {
   constructor(private db: DuckDBConnection) {}

   async get(
      environmentId: string,
      connectionName: string,
      physicalTableName: string,
   ): Promise<IncrementalLedgerEntry | null> {
      const row = await this.db.get<Record<string, unknown>>(
         `SELECT * FROM incremental_ledger
           WHERE environment_id = ? AND connection_name = ?
             AND physical_table_name = ?`,
         [environmentId, connectionName, physicalTableName],
      );
      return row ? mapRow(row) : null;
   }

   /**
    * Record a boundary, replacing whatever this table had before. `created_at`
    * survives a replace (it dates the table's first seed, which is the useful
    * thing to know), while `advanced_at` moves with every write.
    *
    * `package_name` and `source_entity_id` are overwritten with the writer's, so
    * the row always names whoever last advanced it — which is the honest answer
    * for a table that several versions of a package refresh in turn.
    */
   async upsert(
      entry: Omit<IncrementalLedgerEntry, "createdAt" | "advancedAt">,
   ): Promise<IncrementalLedgerEntry> {
      const now = new Date().toISOString();
      const rows = await this.db.all<Record<string, unknown>>(
         `INSERT INTO incremental_ledger (
             environment_id, package_name, source_entity_id,
             covered_through_value, covered_through_type,
             watermark_dimension, merge_key_dimensions, derived_strategy,
             physical_table_name, connection_name,
             advanced_by_materialization_id, advanced_at, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT (environment_id, connection_name, physical_table_name)
          DO UPDATE SET
             package_name = EXCLUDED.package_name,
             source_entity_id = EXCLUDED.source_entity_id,
             covered_through_value = EXCLUDED.covered_through_value,
             covered_through_type = EXCLUDED.covered_through_type,
             watermark_dimension = EXCLUDED.watermark_dimension,
             merge_key_dimensions = EXCLUDED.merge_key_dimensions,
             derived_strategy = EXCLUDED.derived_strategy,
             advanced_by_materialization_id = EXCLUDED.advanced_by_materialization_id,
             advanced_at = EXCLUDED.advanced_at
          RETURNING *`,
         [
            entry.environmentId,
            entry.packageName,
            entry.sourceEntityId,
            entry.coveredThroughValue,
            entry.coveredThroughType,
            entry.watermarkDimension,
            JSON.stringify(entry.mergeKeyDimensions),
            entry.derivedStrategy,
            entry.physicalTableName,
            entry.connectionName,
            entry.advancedByMaterializationId,
            now,
            now,
         ],
      );
      return mapRow(rows[0]);
   }

   /**
    * Forget a table's boundary. Used when a forced full rebuild replaces the
    * table's contents wholesale: the old boundary describes data that no longer
    * exists, and leaving it would let the next delta start mid-table.
    */
   async deleteEntry(
      environmentId: string,
      connectionName: string,
      physicalTableName: string,
   ): Promise<void> {
      await this.db.run(
         `DELETE FROM incremental_ledger
           WHERE environment_id = ? AND connection_name = ?
             AND physical_table_name = ?`,
         [environmentId, connectionName, physicalTableName],
      );
   }

   async deleteByEnvironmentId(environmentId: string): Promise<void> {
      await this.db.run(
         "DELETE FROM incremental_ledger WHERE environment_id = ?",
         [environmentId],
      );
   }

   /**
    * Forget every boundary tagged with a package, for when that package is
    * deleted. Best-effort by nature now that the package is not part of the key:
    * the tag names whoever last advanced a row, so a table shared across versions
    * of a package keeps its boundary if a different version wrote it last. That
    * costs at most one seed, and only if the table survives its package.
    */
   async deleteByPackage(
      environmentId: string,
      packageName: string,
   ): Promise<void> {
      await this.db.run(
         "DELETE FROM incremental_ledger WHERE environment_id = ? AND package_name = ?",
         [environmentId, packageName],
      );
   }
}

function mapRow(row: Record<string, unknown>): IncrementalLedgerEntry {
   return {
      environmentId: row.environment_id as string,
      packageName: row.package_name as string,
      sourceEntityId: row.source_entity_id as string,
      coveredThroughValue: row.covered_through_value as string,
      coveredThroughType: row.covered_through_type as string,
      watermarkDimension: row.watermark_dimension as string,
      mergeKeyDimensions: parseNameList(row.merge_key_dimensions),
      derivedStrategy: row.derived_strategy as IncrementalStrategy,
      physicalTableName: row.physical_table_name as string,
      connectionName: row.connection_name as string,
      advancedByMaterializationId:
         row.advanced_by_materialization_id != null
            ? (row.advanced_by_materialization_id as string)
            : null,
      advancedAt: new Date(row.advanced_at as string),
      createdAt: new Date(row.created_at as string),
   };
}

/**
 * The recorded merge keys. An unreadable value degrades to `[]`, which reads as
 * "range replace" and therefore as a MISMATCH against any merge declaration —
 * the direction that forces a re-seed rather than a merge applied under keys we
 * could not confirm.
 */
function parseNameList(value: unknown): string[] {
   if (value == null) return [];
   if (Array.isArray(value)) return value.map(String);
   try {
      const parsed = JSON.parse(String(value));
      return Array.isArray(parsed) ? parsed.map(String) : [];
   } catch {
      return [];
   }
}
