import type { Connection } from "@malloydata/malloy";
import {
   DatabricksDialect,
   DuckDBDialect,
   MySQLDialect,
   PostgresDialect,
   SnowflakeDialect,
   StandardSQLDialect,
   TrinoDialect,
} from "@malloydata/malloy";
import { logger } from "../logger";
import { ManifestEntry } from "../storage/DatabaseInterface";
import { ManifestService } from "./manifest_service";
import { stagingSuffix } from "./materialization_service";
import { type Quoter, quoteTablePath } from "./quoting";

/**
 * Registry of built-in dialects keyed by `Connection.dialectName`. Malloy's
 * internal `getDialect` helper isn't part of the package's public exports,
 * so we assemble our own registry from the exported dialect classes.
 *
 * Note: `presto` (extends `TrinoDialect`) is not re-exported publicly and
 * is niche enough to omit; if/when it ships as a publisher connection type,
 * add it here.
 */
const DIALECTS: Readonly<Record<string, Quoter>> = Object.freeze({
   duckdb: new DuckDBDialect(),
   standardsql: new StandardSQLDialect(),
   trino: new TrinoDialect(),
   postgres: new PostgresDialect(),
   snowflake: new SnowflakeDialect(),
   mysql: new MySQLDialect(),
   databricks: new DatabricksDialect(),
});

/** Build a stable key for a `(connectionName, tableName)` tuple. */
export function liveTableKey(
   connectionName: string,
   tableName: string,
): string {
   return `${connectionName}::${tableName}`;
}

/** One manifest entry that was successfully GC'd. */
export interface GcDropped {
   buildId: string;
   tableName: string;
   connectionName: string;
   stagingTableName: string;
   /**
    * True when the physical target was left in place because another
    * active manifest entry still claims this `(connection, tableName)`
    * pair. The stale manifest row was still deleted — only the DROP was
    * suppressed.
    */
   targetDropSkipped?: boolean;
}

/** One manifest entry that could not be GC'd. The row is left in place. */
export interface GcError {
   buildId: string;
   tableName: string;
   connectionName: string;
   error: string;
}

export interface GcResult {
   dropped: GcDropped[];
   errors: GcError[];
}

export interface GcContext {
   connections: Map<string, Connection>;
   manifestService: ManifestService;
   environmentId: string;
   dryRun?: boolean;
   /**
    * Set of `liveTableKey(connectionName, tableName)` tuples that some
    * *active* manifest entry still claims. When a stale entry's target
    * is in this set the physical DROP is skipped (the fresh row needs
    * the table); the stale manifest row is still deleted and the
    * staging companion is still best-effort dropped.
    */
   liveTables?: ReadonlySet<string>;
   /**
    * When true, delete the manifest row even if the row's
    * `connectionName` is no longer registered with the package. Used by
    * `teardownPackage` where retaining rows that point at a vanished
    * connection just makes them un-GC-able.
    */
   forceDeleteRowOnMissingConnection?: boolean;
}

/**
 * Process a single manifest entry for GC. Returns either a `dropped` or
 * `error` result (never both).
 *
 * Ordering invariant: the manifest row is deleted **before** any physical
 * DROP so a failed DROP leaves an orphaned table (recoverable) rather
 * than an orphaned manifest row (causes skipped builds + query failures).
 */
async function processOneEntry(
   entry: ManifestEntry,
   ctx: GcContext,
   liveTables: ReadonlySet<string>,
): Promise<{ dropped?: GcDropped; error?: GcError }> {
   const stagingTableName = `${entry.tableName}${stagingSuffix(entry.buildId)}`;
   const targetIsLive = liveTables.has(
      liveTableKey(entry.connectionName, entry.tableName),
   );

   const connection = ctx.connections.get(entry.connectionName);

   // ── Missing connection ────────────────────────────────────────
   if (!connection) {
      if (ctx.forceDeleteRowOnMissingConnection && !ctx.dryRun) {
         try {
            await ctx.manifestService.deleteEntry(ctx.environmentId, entry.id);
            logger.warn(
               "GC: deleted manifest row whose connection is gone; physical table (if any) is orphaned",
               {
                  manifestEntryId: entry.id,
                  tableName: entry.tableName,
                  connectionName: entry.connectionName,
               },
            );
            return {
               dropped: {
                  buildId: entry.buildId,
                  tableName: entry.tableName,
                  connectionName: entry.connectionName,
                  stagingTableName,
                  targetDropSkipped: true,
               },
            };
         } catch (err) {
            return {
               error: {
                  buildId: entry.buildId,
                  tableName: entry.tableName,
                  connectionName: entry.connectionName,
                  error: err instanceof Error ? err.message : String(err),
               },
            };
         }
      }
      return {
         error: {
            buildId: entry.buildId,
            tableName: entry.tableName,
            connectionName: entry.connectionName,
            error: `Connection '${entry.connectionName}' is not available`,
         },
      };
   }

   // ── Unknown dialect ───────────────────────────────────────────
   const dialect = DIALECTS[connection.dialectName];
   if (!dialect) {
      return {
         error: {
            buildId: entry.buildId,
            tableName: entry.tableName,
            connectionName: entry.connectionName,
            error: `No dialect registered for '${connection.dialectName}'`,
         },
      };
   }

   // ── Dry run ───────────────────────────────────────────────────
   if (ctx.dryRun) {
      return {
         dropped: {
            buildId: entry.buildId,
            tableName: entry.tableName,
            connectionName: entry.connectionName,
            stagingTableName,
            targetDropSkipped: targetIsLive || undefined,
         },
      };
   }

   // ── Live run: delete manifest row first ───────────────────────
   const quoted = (p: string) => quoteTablePath(p, dialect);

   try {
      await ctx.manifestService.deleteEntry(ctx.environmentId, entry.id);
   } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      logger.warn("GC: failed to delete manifest row; skipping physical drop", {
         manifestEntryId: entry.id,
         tableName: entry.tableName,
         error,
      });
      return {
         error: {
            buildId: entry.buildId,
            tableName: entry.tableName,
            connectionName: entry.connectionName,
            error,
         },
      };
   }

   // ── Drop target table (unless still live) ─────────────────────
   if (targetIsLive) {
      logger.info(
         "GC: skipping target DROP; another active manifest entry claims this (connection, tableName)",
         {
            tableName: entry.tableName,
            connectionName: entry.connectionName,
            retiredBuildId: entry.buildId,
         },
      );
   } else {
      try {
         await connection.runSQL(
            `DROP TABLE IF EXISTS ${quoted(entry.tableName)}`,
         );
      } catch (err) {
         logger.warn(
            "GC: deleted manifest row but failed to drop materialized table (orphaned)",
            {
               tableName: entry.tableName,
               connectionName: entry.connectionName,
               error: err instanceof Error ? err.message : String(err),
            },
         );
      }
   }

   // ── Best-effort drop staging companion ────────────────────────
   try {
      await connection.runSQL(
         `DROP TABLE IF EXISTS ${quoted(stagingTableName)}`,
      );
   } catch (err) {
      logger.warn("GC: failed to drop staging table (best-effort)", {
         stagingTableName,
         connectionName: entry.connectionName,
         error: err instanceof Error ? err.message : String(err),
      });
   }

   return {
      dropped: {
         buildId: entry.buildId,
         tableName: entry.tableName,
         connectionName: entry.connectionName,
         stagingTableName,
         targetDropSkipped: targetIsLive || undefined,
      },
   };
}

/**
 * Idempotent manifest-row-delete + DROP TABLE for each entry.
 *
 * The manifest row is deleted **before** the physical tables so a failed
 * DROP leaves an orphaned table (recoverable by the next build that
 * targets the same tableName) rather than an orphaned manifest row
 * (causes skipped builds + query failures).
 */
export async function dropManifestEntries(
   entries: ManifestEntry[],
   ctx: GcContext,
): Promise<GcResult> {
   const dropped: GcDropped[] = [];
   const errors: GcError[] = [];
   const liveTables = ctx.liveTables ?? new Set<string>();

   for (const entry of entries) {
      const result = await processOneEntry(entry, ctx, liveTables);
      if (result.dropped) dropped.push(result.dropped);
      if (result.error) errors.push(result.error);
   }

   return { dropped, errors };
}
