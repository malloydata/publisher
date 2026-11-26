// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Whether a database table's container exists: the dataset or schema its
 * `name=` places it in ({@link readTargetContainers}, at plan;
 * {@link readTargetContainer}, one container, before a build writes
 * anything). No build creates a container (its owner does), so without this
 * a typo, or a dataset in another location, fails every build of the table
 * with the warehouse's own "not found".
 *
 * - BigQuery: the dataset's metadata (`dataset(id, {projectId}).getMetadata()`),
 *   in the project the container names, else the connection's. A dataset in
 *   another location than the connection declares reads `otherLocation`.
 * - Postgres: `pg_namespace` (every schema, not only those the user holds a
 *   privilege on), in the connection's database.
 * - Snowflake: `information_schema.schemata`, of the database the container
 *   names, else the connection's.
 * - DuckDB: `duckdb_schemas()`, where a one-part container may also name an
 *   attached database, as `CREATE TABLE db.t` reads it.
 *
 * Names are compared as the build quotes them: exactly, except on DuckDB,
 * which resolves even a quoted identifier case-insensitively. Any other
 * dialect, and any shape this does not read (a Postgres container in another
 * database), is not checked. A read that fails, or outlives its timeout,
 * refuses nothing: it is logged, and the build finds out. Never throws.
 */

import type { RunSQLOptions } from "@malloydata/malloy";
import { errorCodeOf } from "../../../error_code";
import { logger } from "../../../logger";
import { runAsInternalStatement } from "../../duckdb_lockdown";
import { capabilitiesOf } from "../domain/dialect";
import {
   type TargetContainer,
   type TargetContainerConnection,
   targetContainerKey,
   type TargetContainerReading,
   type TargetContainerReadings,
} from "../domain/target_containers";

/** How long one container read may take: the upstream schema read's bound. */
const CONTAINER_READ_TIMEOUT_MS = 15_000;

/** Container reads in flight at once, across a plan's connections. */
const CONTAINER_READ_CONCURRENCY = 4;

/**
 * The reading of each of `containers`, by `targetContainerKey`: each read
 * once, a failed read (its connection included) read as `unread`.
 */
export async function readTargetContainers(args: {
   containers: readonly TargetContainer[];
   connection: (connectionName: string) => Promise<TargetContainerConnection>;
   /** Overrides the read's timeout (a test's). */
   timeoutMs?: number;
}): Promise<TargetContainerReadings> {
   const out: TargetContainerReadings = {};
   const distinct = new Map(
      args.containers.map((c) => [targetContainerKey(c), c]),
   );
   const tasks = [...distinct].map(([key, target]) => async () => {
      out[key] = await readTargetContainerOrUnread(
         () => args.connection(target.connectionName),
         target,
         { timeoutMs: args.timeoutMs },
      );
   });
   await runBounded(tasks, CONTAINER_READ_CONCURRENCY);
   return out;
}

/**
 * {@link readTargetContainer} under its timeout, a read that fails (its
 * connection included) logged and read as `unread`. Never throws.
 */
export async function readTargetContainerOrUnread(
   connection: () => Promise<TargetContainerConnection>,
   target: TargetContainer,
   options: { runOptions?: RunSQLOptions; timeoutMs?: number } = {},
): Promise<TargetContainerReading> {
   try {
      return await withTimeout(
         (async () =>
            readTargetContainer(
               await connection(),
               target.container,
               options.runOptions,
            ))(),
         options.timeoutMs ?? CONTAINER_READ_TIMEOUT_MS,
      );
   } catch (err) {
      logger.warn(
         "A table's container could not be read; nothing is refused or stopped for it",
         { connectionName: target.connectionName, code: errorCodeOf(err) },
      );
      return { unread: true };
   }
}

/**
 * Whether `connection` holds `container` (see the file comment). Throws when
 * the read itself fails; a dialect or shape it does not read is `unread`.
 */
export async function readTargetContainer(
   connection: TargetContainerConnection,
   container: string,
   runOptions?: RunSQLOptions,
): Promise<TargetContainerReading> {
   const parts = container.split(".").map(unquoted);
   if (parts.some((p) => p === "")) return { unread: true };
   switch (capabilitiesOf(connection.dialectName).containerCatalog) {
      case "dataset":
         return readBigQueryDataset(connection, container);
      case "pg_namespace": {
         if (parts.length > 2) return { unread: true };
         const [database, schema] =
            parts.length === 2 ? parts : [undefined, parts[0]];
         const row = await countRow(
            connection,
            // Wrapped in row_to_json: Malloy's Postgres runSQL returns each
            // row's `row` column, as warehouse_sql.ts's catalog reads are.
            `SELECT row_to_json(p) AS "row" FROM (SELECT current_database() AS db, (SELECT COUNT(*) FROM pg_catalog.pg_namespace WHERE nspname = ${literal(schema)}) AS n) AS p`,
            runOptions,
         );
         // A container in another database is not one this connection
         // writes to, and not this read's to judge.
         if (
            row === undefined ||
            (database !== undefined && String(row.db) !== database)
         ) {
            return { unread: true };
         }
         return answer(row);
      }
      case "schemata": {
         if (parts.length > 2) return { unread: true };
         const [database, schema] =
            parts.length === 2 ? parts : [undefined, parts[0]];
         const catalog =
            database === undefined
               ? "information_schema"
               : `"${database.replace(/"/g, '""')}".information_schema`;
         return answer(
            await countRow(
               connection,
               `SELECT COUNT(*) AS n FROM ${catalog}.schemata WHERE schema_name = ${literal(schema)}`,
               runOptions,
            ),
         );
      }
      case "duckdb_schemas": {
         if (parts.length > 2) return { unread: true };
         const where =
            parts.length === 2
               ? `lower(database_name) = lower(${literal(parts[0])}) AND lower(schema_name) = lower(${literal(parts[1])})`
               : `(database_name = current_database() AND lower(schema_name) = lower(${literal(parts[0])})) OR lower(database_name) = lower(${literal(parts[0])})`;
         const sql = `SELECT COUNT(*) AS n FROM duckdb_schemas() WHERE ${where}`;
         return answer(
            await runAsInternalStatement(sql, () =>
               countRow(connection, sql, runOptions),
            ),
         );
      }
      default:
         return { unread: true };
   }
}

/** The BigQuery client fields a dataset read needs (private on the connection). */
interface BigQueryInternals {
   bigQuery?: {
      dataset(
         id: string,
         options?: { projectId?: string },
      ): { getMetadata(): Promise<[{ location?: unknown }, ...unknown[]]> };
   };
   projectId?: unknown;
   location?: unknown;
}

/**
 * A BigQuery dataset's existence and location. The container is a dataset,
 * or a project and dataset split on the last dot, so a `domain.com:proj`
 * project id survives.
 */
async function readBigQueryDataset(
   connection: TargetContainerConnection,
   container: string,
): Promise<TargetContainerReading> {
   const internals = connection as unknown as BigQueryInternals;
   const client = internals.bigQuery;
   if (!client || typeof client.dataset !== "function") {
      return { unread: true };
   }
   const lastDot = container.lastIndexOf(".");
   const dataset = unquoted(container.slice(lastDot + 1));
   const named = lastDot > 0 ? unquoted(container.slice(0, lastDot)) : "";
   const project =
      named !== ""
         ? named
         : typeof internals.projectId === "string"
           ? internals.projectId
           : undefined;
   let metadata: { location?: unknown };
   try {
      [metadata] = await client
         .dataset(dataset, project !== undefined ? { projectId: project } : {})
         .getMetadata();
   } catch (err) {
      if (isNotFound(err)) {
         return {
            notFound: true,
            ...(named === "" && project !== undefined ? { project } : {}),
         };
      }
      throw err;
   }
   const expected =
      typeof internals.location === "string" ? internals.location : "";
   const location =
      typeof metadata?.location === "string" ? metadata.location : "";
   if (
      expected !== "" &&
      location !== "" &&
      expected.toLowerCase() !== location.toLowerCase()
   ) {
      return { otherLocation: { location, expected } };
   }
   return { found: true };
}

/** Whether a Google API error says the resource does not exist. */
function isNotFound(err: unknown): boolean {
   const e = err as { code?: unknown; errors?: unknown };
   if (e?.code === 404) return true;
   return (
      Array.isArray(e?.errors) &&
      e.errors.some((x) => (x as { reason?: unknown })?.reason === "notFound")
   );
}

/**
 * The one row of a count query (`n`, the containers found), its column read
 * whatever case the warehouse spells it in; undefined when no row came back.
 */
async function countRow(
   connection: TargetContainerConnection,
   sql: string,
   runOptions: RunSQLOptions | undefined,
): Promise<Record<string, unknown> | undefined> {
   const result = await connection.runSQL(sql, {
      ...runOptions,
      rowLimit: 1,
   });
   const row = (result.rows ?? [])[0] as Record<string, unknown> | undefined;
   if (row === undefined) return undefined;
   return Object.fromEntries(
      Object.entries(row).map(([k, v]) => [k.toLowerCase(), v]),
   );
}

/**
 * A count row's reading: a count of none is `notFound`, of any `found`,
 * anything unreadable (no row, no count) `unread`.
 */
function answer(
   row: Record<string, unknown> | undefined,
): TargetContainerReading {
   const n = row === undefined ? NaN : Number(row.n);
   if (!Number.isFinite(n)) return { unread: true };
   return n > 0 ? { found: true } : { notFound: true };
}

/** A SQL string literal. */
function literal(value: string): string {
   return `'${value.replace(/'/g, "''")}'`;
}

/** A name segment without the quotes a `name=` may carry around it. */
function unquoted(segment: string): string {
   const s = segment.trim();
   return /^(`.*`|".*")$/.test(s) ? s.slice(1, -1) : s;
}

/** A read that outlived its bound. */
class ContainerReadTimeout extends Error {
   constructor(ms: number) {
      super(`the container read timed out after ${ms}ms`);
      this.name = "ContainerReadTimeout";
   }
}

/** `promise`, or a rejection after `ms`. */
function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
   let timer: ReturnType<typeof setTimeout> | undefined;
   return Promise.race([
      promise,
      new Promise<never>((_, reject) => {
         timer = setTimeout(() => reject(new ContainerReadTimeout(ms)), ms);
      }),
   ]).finally(() => clearTimeout(timer));
}

/** Run `tasks`, at most `limit` at once. */
async function runBounded(
   tasks: (() => Promise<void>)[],
   limit: number,
): Promise<void> {
   let next = 0;
   const worker = async () => {
      while (next < tasks.length) {
         const task = tasks[next++];
         await task();
      }
   };
   await Promise.all(
      Array.from({ length: Math.min(limit, tasks.length) }, worker),
   );
}
