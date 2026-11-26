// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   buildPoolOptions,
   SnowflakeConnection,
} from "@malloydata/db-snowflake";
import type {
   MalloyQueryData,
   QueryRecord,
   QueryValue,
   RunSQLOptions,
   SQLSourceDef,
   SQLSourceRequest,
} from "@malloydata/malloy";
import type { components } from "../api";
import { removeUndefined } from "./connection_config";
import { normalizeSnowflakePrivateKey } from "./dialects/snowflake";

type ApiSnowflakeConnection = components["schemas"]["SnowflakeConnection"];
type SnowflakeConnOptions = NonNullable<
   NonNullable<
      ConstructorParameters<typeof SnowflakeConnection>[1]
   >["connOptions"]
>;

/**
 * The connection options of every Snowflake connection Publisher builds,
 * password or key pair.
 *
 * Integers are read as the connector's own default reads them
 * (`jsTreatIntegerAsBigInt`, `SnowflakeExecutor.defaultConnectionOptions`),
 * which the connector applies only to options it reads from the environment
 * or a TOML file, never to options it is handed. Without it snowflake-sdk
 * reads a `NUMBER(p,0)` through a JavaScript number, so a value past 2^53
 * comes back rounded while Malloy's dialect says it is exact.
 *
 * Key-pair auth is chosen by presence: a private key selects `SNOWFLAKE_JWT`
 * (a password, if also set, is passed along and unused). Unset and `null`
 * optional fields are dropped: a surviving `null` reaches `makeDigest`, which
 * throws on it.
 */
export function buildSnowflakeConnOptions(
   name: string,
   snowflake: ApiSnowflakeConnection | undefined,
): SnowflakeConnOptions {
   if (!snowflake?.account) {
      throw new Error(`Snowflake account is required for connection ${name}`);
   }
   if (!snowflake.username) {
      throw new Error(`Snowflake username is required for connection ${name}`);
   }
   if (!snowflake.warehouse) {
      throw new Error(`Snowflake warehouse is required for connection ${name}`);
   }
   if (!snowflake.privateKey && !snowflake.password) {
      throw new Error(
         `Snowflake privateKey or password is required for connection ${name}`,
      );
   }
   return {
      account: snowflake.account,
      username: snowflake.username,
      warehouse: snowflake.warehouse,
      ...(snowflake.privateKey
         ? {
              privateKey: normalizeSnowflakePrivateKey(snowflake.privateKey),
              authenticator: "SNOWFLAKE_JWT",
           }
         : {}),
      ...removeUndefined({
         password: snowflake.password,
         privateKeyPass: snowflake.privateKeyPass,
         database: snowflake.database,
         schema: snowflake.schema,
         role: snowflake.role,
      }),
      jsTreatIntegerAsBigInt: true,
   };
}

/** Builds the Snowflake connection a connection config names. */
export function buildSnowflakeConnection(
   name: string,
   snowflake: ApiSnowflakeConnection | undefined,
): SnowflakeConnection {
   return new PublisherSnowflakeConnection(name, {
      connOptions: buildSnowflakeConnOptions(name, snowflake),
      timeoutMs: snowflake?.responseTimeoutMilliseconds ?? undefined,
      // Server-owned pool sizing, not in the API.
      poolOptions: buildPoolOptions({ poolMin: 1, poolMax: 20 }),
   });
}

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * One value snowflake-sdk returned, in the form DuckDB's connection returns
 * it, which is what a stored copy answers from.
 *
 * - An integer arrives as the driver's `big-integer` wrapper (it carries the
 *   exact value as a native BigInt in `.value`). A safe integer becomes a
 *   number, as without `jsTreatIntegerAsBigInt`; a larger one a BigInt, as
 *   DuckDB returns a BIGINT, so Malloy's result keeps every digit and a
 *   compact answer gives it as a string (`json_utils.bigIntReplacer`).
 * - A DATE or TIMESTAMP_* value arrives as a Date whose own `toJSON` renders
 *   it in the session's output format (`2026-01-15 10:00:00.000 -0800` for a
 *   TIMESTAMP_TZ, `2026-01-15` for a DATE). It becomes a plain Date of the
 *   same instant, so it serializes as ISO 8601 in UTC, as a stored copy's
 *   value does.
 *
 * Plain arrays and objects (a structured column) are walked; anything else is
 * returned as it is.
 */
export function normalizeSnowflakeValue(value: unknown): unknown {
   if (value === null || typeof value !== "object") return value;
   if (value instanceof Date) {
      return Object.hasOwn(value, "toJSON") ? new Date(value.getTime()) : value;
   }
   if (Array.isArray(value)) return value.map(normalizeSnowflakeValue);
   const exact = (value as { value?: unknown }).value;
   if (typeof exact === "bigint" && !isPlainObject(value)) {
      return exact > MAX_SAFE || exact < -MAX_SAFE ? exact : Number(exact);
   }
   if (isPlainObject(value)) {
      return Object.fromEntries(
         Object.entries(value).map(([k, v]) => [k, normalizeSnowflakeValue(v)]),
      );
   }
   return value;
}

function isPlainObject(value: object): boolean {
   const proto = Object.getPrototypeOf(value);
   return proto === Object.prototype || proto === null;
}

function normalizeRecord(row: QueryRecord): QueryRecord {
   const out: QueryRecord = {};
   for (const [key, value] of Object.entries(row)) {
      out[key] = normalizeSnowflakeValue(value) as QueryValue;
   }
   return out;
}

/**
 * The parts of `@malloydata/db-snowflake`'s `SnowflakeExecutor` that
 * {@link pinnedExecutor} reaches. They are private in its typings, so each is
 * checked before use and a connector that lacks one is left unpinned.
 */
interface SnowflakePool<C> {
   acquire(): Promise<C>;
   release(conn: C): Promise<void>;
   destroy(conn: C): Promise<void>;
}
interface SnowflakeExecutorInternals<C> {
   pool_: SnowflakePool<C>;
   ensureSessionInitialized(
      conn: C,
      options?: RunSQLOptions,
      timeoutMs?: number,
   ): Promise<void>;
   _execute(
      sql: string,
      conn: C,
      options?: RunSQLOptions,
      timeoutMs?: number,
      binds?: unknown,
   ): Promise<QueryRecord[]>;
}
type BatchFn = (
   sql: string,
   options?: RunSQLOptions,
   timeoutMs?: number,
   binds?: unknown,
) => Promise<QueryRecord[] | undefined>;

function executorInternals(
   executor: unknown,
): SnowflakeExecutorInternals<unknown> | undefined {
   const e = executor as Partial<SnowflakeExecutorInternals<unknown>>;
   const pool = e?.pool_ as Partial<SnowflakePool<unknown>> | undefined;
   return typeof pool?.acquire === "function" &&
      typeof pool.release === "function" &&
      typeof pool.destroy === "function" &&
      typeof e.ensureSessionInitialized === "function" &&
      typeof e._execute === "function"
      ? (e as SnowflakeExecutorInternals<unknown>)
      : undefined;
}

/**
 * A pinned schema read reached an executor method Publisher does not pin: a
 * connector newer than this mitigation. The read is retried unpinned.
 */
class UnpinnedExecutorMethodError extends Error {
   constructor(readonly method: string) {
      super(
         `the Snowflake schema read called executor.${method}, which Publisher ` +
            `does not pin to one session`,
      );
   }
}

/**
 * An executor that runs every statement on the one session `conn`, with the
 * connector's own session setup and the same semantics as its pooled calls:
 * `batch`, `tryBatch` (undefined on a failure), `stream` (the batch's rows,
 * up to `rowLimit`), `_execute` (already per session) and `done` (a no-op:
 * the shared executor owns the pool). Any other method a newer connector
 * calls throws {@link UnpinnedExecutorMethodError} rather than reaching the
 * shared executor or failing as a `TypeError`.
 */
function pinnedExecutor(
   base: SnowflakeExecutorInternals<unknown>,
   conn: unknown,
): object {
   const batch: BatchFn = async (sql, options, timeoutMs, binds) => {
      await base.ensureSessionInitialized(conn, options, timeoutMs);
      return base._execute(sql, conn, options, timeoutMs, binds);
   };
   const pinned: Record<string, unknown> = {
      batch,
      tryBatch: (async (sql, options, timeoutMs, binds) => {
         try {
            return await batch(sql, options, timeoutMs, binds);
         } catch {
            return undefined;
         }
      }) satisfies BatchFn,
      stream: async (sql: string, options?: RunSQLOptions) => {
         const rows = (await batch(sql, options)) ?? [];
         const limit = options?.rowLimit ?? rows.length;
         return (async function* () {
            yield* rows.slice(0, limit);
         })();
      },
      _execute: base._execute.bind(base),
      done: async () => {},
   };
   return new Proxy(pinned, {
      get(target, prop, receiver) {
         if (typeof prop === "string" && !(prop in target)) {
            const value = (base as unknown as Record<string, unknown>)[prop];
            if (typeof value === "function") {
               throw new UnpinnedExecutorMethodError(prop);
            }
            return value;
         }
         return Reflect.get(target, prop, receiver);
      },
   });
}

/**
 * A Snowflake connection whose rows hold the values a stored copy's do
 * ({@link normalizeSnowflakeValue}), for query answers and Publisher's own
 * warehouse reads alike.
 */
export class PublisherSnowflakeConnection extends SnowflakeConnection {
   /**
    * R-08 MITIGATION for an upstream race in `@malloydata/db-snowflake`
    * (`SnowflakeConnection.fetchSelectSchema`): a `.sql()` source's schema is
    * read by creating a session-scoped `TEMP VIEW tt<hash>` and then running
    * `DESCRIBE TABLE` (and, for a VARIANT column, sampling queries) against
    * it, each statement on its own `pool.use()`. The pool is generic-pool,
    * FIFO, and Publisher sizes it up to 20, so once it holds more than one
    * idle session the `DESCRIBE` is handed a different session from the one
    * that created the view, and the publish fails with "Table 'TT…' does not
    * exist or not authorized". Here the whole read runs on one session
    * borrowed for it, released afterwards, or destroyed on a failure exactly
    * as `pool.use()` would. A connector whose executor does not expose the
    * internals this needs, or whose read calls an executor method this does
    * not pin, is read the upstream way, unpinned. Remove once the
    * connector pins the read itself.
    */
   async fetchSelectSchema(sqlRef: SQLSourceRequest): Promise<SQLSourceDef> {
      const base = executorInternals(
         (this as unknown as { executor: unknown }).executor,
      );
      if (!base) return super.fetchSelectSchema(sqlRef);
      const conn = await base.pool_.acquire();
      // The upstream method reads `this.executor` for every statement, its
      // `runSQL` (ours, which normalizes rows) included; a per-call view of
      // this connection with the pinned executor keeps concurrent reads
      // apart without touching the shared executor.
      const pinned = Object.create(this, {
         executor: { value: pinnedExecutor(base, conn) },
      }) as this;
      let result: SQLSourceDef;
      try {
         result = await super.fetchSelectSchema.call(pinned, sqlRef);
      } catch (error) {
         if (error instanceof UnpinnedExecutorMethodError) {
            // A newer connector's read this cannot pin: read unpinned, as
            // the connector does, on a session left as it was.
            await base.pool_.release(conn).catch(() => {});
            return super.fetchSelectSchema(sqlRef);
         }
         await base.pool_.destroy(conn).catch(() => {});
         throw error;
      }
      await base.pool_.release(conn).catch(() => {});
      return result;
   }

   async runSQL(
      sql: string,
      options?: RunSQLOptions,
   ): Promise<MalloyQueryData> {
      const result = await super.runSQL(sql, options);
      return { ...result, rows: result.rows.map(normalizeRecord) };
   }

   async *runSQLStream(
      sqlCommand: string,
      options?: RunSQLOptions,
   ): AsyncIterableIterator<QueryRecord> {
      for await (const row of super.runSQLStream(sqlCommand, options)) {
         yield normalizeRecord(row);
      }
   }
}
