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
 * table again. A read whose catalog query or connection failed is retried
 * with backoff, within one read's timeout. A read that still fails keeps the
 * table's last reading, so a transient failure never moves an address; with
 * none, the table reads `read_failed`, and the plan refuses each source that
 * reads it (`upstream_unread`) rather than address it, until a read
 * succeeds. A table the dialect gives no catalog to read reads `no_catalog`,
 * one the catalog does not hold `not_found`, and an unqualified name it holds
 * in more than one schema `ambiguous`: each a stable fold. Never throws.
 */

import { errorCodeOf } from "../../../error_code";
import { logger } from "../../../logger";
import {
   type UpstreamSchemaReading,
   type UpstreamTable,
   upstreamTableKey,
   type UpstreamReadConnection,
} from "../domain/upstream_schemas";
import { readTableColumns } from "./database_column_read";
import { NATIVE_SCHEMA_ROW_LIMIT } from "./database_native_types";
import { relationDescribeSQL } from "./warehouse_sql";

/**
 * How long a `DESCRIBE` or a catalog read may take before it reads as
 * failed; a failed read's retries stay within it too.
 */
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
   const timeoutMs = args.timeoutMs ?? DESCRIBE_TIMEOUT_MS;
   const started = Date.now();
   let pending = toRead;
   for (let attempt = 0; ; attempt++) {
      const read = await readOnce(pending, connections, timeoutMs);
      // A read that failed for a reason a retry may clear (the catalog query
      // or the connection failed, not a timeout, which spent the budget, nor
      // a table not found) is read again, after a backoff, while the retries
      // stay within one read's timeout.
      const delay = RETRY_DELAYS_MS[attempt];
      const retry = pending.filter(({ key }) => read.get(key)!.retryable);
      if (
         retry.length > 0 &&
         delay !== undefined &&
         Date.now() - started + delay < timeoutMs
      ) {
         for (const t of pending) {
            if (!read.get(t.key)!.retryable) settle(t, read.get(t.key)!, out);
         }
         await new Promise((resolve) => setTimeout(resolve, delay));
         pending = retry;
         continue;
      }
      for (const t of pending) settle(t, read.get(t.key)!, out);
      return out;
   }

   /** Record `t`'s reading, keeping its last reading in place of a failure. */
   function settle(
      t: (typeof toRead)[number],
      { reading: read, reason }: TableRead,
      into: Record<string, UpstreamSchemaReading>,
   ): void {
      const readings = readingsOf(connections.get(t.table.connectionName)!);
      let reading = read;
      if ("columns" in reading) {
         reading = {
            ...reading,
            dialect: connections.get(t.table.connectionName)!.dialectName,
         };
         readings.set(t.cached, reading);
      } else if (reading.unread === "read_failed") {
         const last = readings.get(t.cached);
         logger.warn(
            last
               ? "Upstream table's columns could not be read; keeping its last reading"
               : "Upstream table's columns could not be read; the sources that read it are refused until a read succeeds",
            { connectionName: t.table.connectionName, reason },
         );
         if (last) reading = last;
      }
      into[t.key] = reading;
   }
}

/** The pause before each retry of a failed catalog read, in order. */
const RETRY_DELAYS_MS = [250, 1_000];

/** One table's read: its reading, why it failed, and whether a retry may help. */
interface TableRead {
   reading: UpstreamSchemaReading;
   reason?: string;
   retryable: boolean;
}

/** Read each of `tables` once, from its catalog or by DuckDB's `DESCRIBE`. */
async function readOnce(
   tables: readonly { key: string; table: UpstreamTable }[],
   connections: ReadonlyMap<string, UpstreamReadConnection | undefined>,
   timeoutMs: number,
): Promise<Map<string, TableRead>> {
   // A dialect that reads any relation as Malloy reads its schema (DuckDB's
   // DESCRIBE, in process) reads every table so; the rest read the catalog.
   const describeOf = (table: UpstreamTable) =>
      relationDescribeSQL(
         table.table,
         connections.get(table.connectionName)!.dialectName,
      );
   const catalog = await readTableColumns({
      tables: tables
         .filter(({ table }) => describeOf(table) === undefined)
         .map(({ key, table }) => ({
            key,
            connectionName: table.connectionName,
            tableName: table.table,
         })),
      connection: async (name) => connections.get(name)!,
      // A model's `connection.table('...')` path, resolved as SQL resolves it.
      foldUnquoted: true,
      timeoutMs,
   });
   const out = new Map<string, TableRead>();
   for (const { key, table } of tables) {
      const describe = describeOf(table);
      if (describe !== undefined) {
         const reading = await describeRelation(
            connections.get(table.connectionName)!,
            describe,
            timeoutMs,
         );
         out.set(key, {
            reading,
            ...("unread" in reading ? { reason: "describe_failed" } : {}),
            retryable: "unread" in reading,
         });
         continue;
      }
      const observed = catalog.get(key);
      if (observed && "columns" in observed) {
         out.set(key, {
            reading: {
               columns: observed.columns.map((c) => ({
                  name: c.name,
                  nativeType: c.nativeType ?? c.type ?? "",
               })),
            },
            retryable: false,
         });
      } else if (observed?.unread === "unparseable") {
         out.set(key, { reading: { unread: "no_catalog" }, retryable: false });
      } else if (
         observed?.unread === "not_found" ||
         observed?.unread === "ambiguous"
      ) {
         // What the catalog holds, not a failure to read it: a stable reading.
         out.set(key, {
            reading: { unread: observed.unread },
            retryable: false,
         });
      } else {
         const reason = observed?.unread ?? "not_read";
         out.set(key, {
            reading: { unread: "read_failed" },
            reason,
            retryable:
               reason === "catalog_query_failed" ||
               reason === "connection_failed",
         });
      }
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
   timeoutMs: number,
): Promise<UpstreamSchemaReading> {
   let timer: ReturnType<typeof setTimeout> | undefined;
   try {
      const result = await Promise.race([
         connection.runSQL(describe, { rowLimit: NATIVE_SCHEMA_ROW_LIMIT }),
         new Promise<never>((_, reject) => {
            timer = setTimeout(
               () => reject(new Error("the describe timed out")),
               timeoutMs,
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
         code: errorCodeOf(err),
      });
      return { unread: "read_failed" };
   } finally {
      clearTimeout(timer);
   }
}
