// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package load's read of the warehouse column types of the tables its
 * persist sources read ({@link readUpstreamSchemas}), which every content
 * address folds in (`domain/upstream_schemas.ts`), so a column retyped
 * upstream re-addresses the sources that read it and their tables are
 * rebuilt.
 *
 * The catalog queries are the bind's (`database_column_read.ts`): one per
 * schema or dataset, bounded concurrency, each under a timeout. A DuckDB
 * relation (a table, a file, a URL) is read as Malloy reads its schema,
 * `DESCRIBE SELECT * FROM <path>`, in process.
 *
 * Readings are kept in this process, by connection (the object), its digest
 * and the table, as Malloy keeps a table's schema: a load reads a table it has no
 * reading of, and an explicit reload or publish (`refresh`) reads every
 * table again. A read that fails keeps the table's last reading, so a
 * transient failure never moves an address; with none, the table reads
 * `read_failed`, a stable value a later successful read moves from once. A
 * table the dialect gives no catalog to read reads `no_catalog`. Never
 * throws.
 */

import type { Connection } from "@malloydata/malloy";
import { logger } from "../../../logger";
import { errMessage } from "../../../utils";
import {
   type UpstreamSchemaReading,
   type UpstreamTable,
   upstreamTableKey,
} from "../domain/upstream_schemas";
import { redactConnectionSecrets } from "./connection_redaction";
import { readTableColumns } from "./database_column_read";
import { NATIVE_SCHEMA_ROW_LIMIT } from "./database_native_types";
import { relationDescribeSQL } from "./warehouse_sql";

/** What a reading needs of a connection. */
type UpstreamReadConnection = Pick<Connection, "runSQL" | "dialectName"> & {
   getDigest?(): string | Promise<string>;
};

/** How long a `DESCRIBE` may take before it reads as failed. */
const DESCRIBE_TIMEOUT_MS = 15_000;

type ColumnsReading = Extract<UpstreamSchemaReading, { columns: unknown }>;

/**
 * The last reading of each table that held columns, by the connection object
 * that read it and then {@link cacheKey}. By the object, not its name: two
 * packages' DuckDB sandboxes share a name and a digest, yet a relative path
 * names a file in each package's own directory.
 */
const lastReadings = new WeakMap<object, Map<string, ColumnsReading>>();

function cacheKey(digest: string, table: string): string {
   return JSON.stringify([digest, table]);
}

/** The readings `connection` has made, by {@link cacheKey}. */
function readingsOf(connection: object): Map<string, ColumnsReading> {
   let readings = lastReadings.get(connection);
   if (!readings) {
      readings = new Map();
      lastReadings.set(connection, readings);
   }
   return readings;
}

/**
 * The reading of each of `tables`, by `upstreamTableKey`. `refresh` reads
 * every table from its catalog again (an explicit reload or publish);
 * otherwise a table read before in this process keeps that reading.
 */
export async function readUpstreamSchemas(args: {
   tables: readonly UpstreamTable[];
   connection: (connectionName: string) => Promise<UpstreamReadConnection>;
   refresh: boolean;
   /** Overrides the catalog read's timeout (a test's). */
   timeoutMs?: number;
}): Promise<Record<string, UpstreamSchemaReading>> {
   const out: Record<string, UpstreamSchemaReading> = {};
   const connections = new Map<string, UpstreamReadConnection | undefined>();
   const digests = new Map<string, string>();
   for (const name of new Set(args.tables.map((t) => t.connectionName))) {
      try {
         const connection = await args.connection(name);
         connections.set(name, connection);
         digests.set(name, String((await connection.getDigest?.()) ?? ""));
      } catch {
         connections.set(name, undefined);
      }
   }
   const toRead: { key: string; table: UpstreamTable; cached: string }[] = [];
   for (const table of args.tables) {
      const key = upstreamTableKey(table);
      if (!connections.get(table.connectionName)) {
         out[key] = { unread: "read_failed" };
         continue;
      }
      const cached = cacheKey(digests.get(table.connectionName)!, table.table);
      const last = readingsOf(connections.get(table.connectionName)!).get(
         cached,
      );
      if (last && !args.refresh) {
         out[key] = last;
         continue;
      }
      toRead.push({ key, table, cached });
   }
   if (toRead.length === 0) return out;
   // A dialect that reads any relation as Malloy reads its schema (DuckDB's
   // DESCRIBE, in process) reads every table so; the rest read the catalog.
   const describeOf = (table: UpstreamTable) =>
      relationDescribeSQL(
         table.table,
         connections.get(table.connectionName)!.dialectName,
      );
   const catalog = await readTableColumns({
      tables: toRead
         .filter(({ table }) => describeOf(table) === undefined)
         .map(({ key, table }) => ({
            key,
            connectionName: table.connectionName,
            tableName: table.table,
         })),
      connection: async (name) => connections.get(name)!,
      ...(args.timeoutMs !== undefined ? { timeoutMs: args.timeoutMs } : {}),
   });
   for (const { key, table, cached } of toRead) {
      const describe = describeOf(table);
      const observed = catalog.get(key);
      let reading: UpstreamSchemaReading;
      if (describe !== undefined) {
         reading = await describeRelation(
            connections.get(table.connectionName)!,
            describe,
         );
      } else if (observed && "columns" in observed) {
         reading = {
            columns: observed.columns.map((c) => ({
               name: c.name,
               nativeType: c.nativeType ?? c.type ?? "",
            })),
         };
      } else if (observed?.unread === "unparseable") {
         reading = { unread: "no_catalog" };
      } else {
         reading = { unread: "read_failed" };
      }
      if ("columns" in reading) {
         reading = {
            ...reading,
            dialect: connections.get(table.connectionName)!.dialectName,
         };
         readingsOf(connections.get(table.connectionName)!).set(
            cached,
            reading,
         );
      } else if (reading.unread === "read_failed") {
         const last = readingsOf(connections.get(table.connectionName)!).get(
            cached,
         );
         logger.warn(
            last
               ? "Upstream table's columns could not be read; keeping its last reading"
               : "Upstream table's columns could not be read; its sources are addressed as unread",
            {
               connectionName: table.connectionName,
               reason:
                  observed && "unread" in observed
                     ? observed.unread
                     : "describe_failed",
            },
         );
         if (last) reading = last;
      }
      out[key] = reading;
   }
   return out;
}

/**
 * A relation read by `describe` ({@link relationDescribeSQL}) on the
 * connection's own session, so its lockdown applies as it does to Malloy's
 * own read of the relation's schema.
 */
async function describeRelation(
   connection: UpstreamReadConnection,
   describe: string,
): Promise<UpstreamSchemaReading> {
   let timer: ReturnType<typeof setTimeout> | undefined;
   try {
      const result = await Promise.race([
         connection.runSQL(describe, { rowLimit: NATIVE_SCHEMA_ROW_LIMIT }),
         new Promise<never>((_, reject) => {
            timer = setTimeout(
               () => reject(new Error("the describe timed out")),
               DESCRIBE_TIMEOUT_MS,
            );
         }),
      ]);
      const columns = (result.rows as Record<string, unknown>[])
         .map((r) => ({
            name: String(r.column_name ?? ""),
            nativeType: String(r.column_type ?? ""),
         }))
         .filter((c) => c.name !== "" && c.nativeType !== "");
      return columns.length > 0 ? { columns } : { unread: "read_failed" };
   } catch (err) {
      logger.warn("Upstream relation could not be described", {
         error: redactConnectionSecrets(errMessage(err)),
      });
      return { unread: "read_failed" };
   } finally {
      clearTimeout(timer);
   }
}
