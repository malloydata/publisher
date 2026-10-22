// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Snowflake query jobs: a model query submitted with `asyncExec`, so the
 * request returns as soon as Snowflake accepts it. Snowflake keeps the result
 * for 24 hours and serves it by row range to any session of the same user that
 * names the query id, so any worker can answer a status or page request: the
 * job lives in Snowflake and in a signed job id, not in this process.
 *
 * Each (environment, connection) gets one long-lived session here, kept alive
 * by heartbeat and never logged out: on an account with
 * ABORT_DETACHED_QUERY = TRUE, Snowflake aborts an async query a few minutes
 * after the session that submitted it logs out.
 */

import crypto from "crypto";
import {
   API,
   type Connection,
   type MalloyQueryData,
   type RunSQLOptions,
} from "@malloydata/malloy";
import snowflake from "snowflake-sdk";
import { components } from "../api";
import { ModelNotFoundError } from "../errors";
import { logger } from "../logger";
import { normalizeSnowflakePrivateKey } from "./connection_config";

type SnowflakeConnectionConfig = components["schemas"]["SnowflakeConnection"];

/** What a job id carries, signed so a caller cannot point it at another model's result. */
export interface QueryJobClaims {
   /** Snowflake's query id. */
   queryId: string;
   environment: string;
   package: string;
   model: string;
   connection: string;
}

export type QueryJobState = "running" | "succeeded" | "failed" | "cancelled";

export interface QueryJobStatus {
   state: QueryJobState;
   /** Snowflake's own status, e.g. QUEUED, RUNNING, SUCCESS, FAILED_WITH_ERROR. */
   snowflakeStatus: string;
   error?: { code?: string; message: string };
}

/** A job id that is malformed, forged, or issued for another model. */
export class QueryJobNotFoundError extends ModelNotFoundError {}

/** Snowflake's error code for a statement cancelled by SYSTEM$CANCEL_QUERY. */
const CANCELLED_CODE = "000604";

/**
 * Malloy's own session conventions (db-snowflake's `_setSessionParams`), so a
 * job's SQL means here what it means on Malloy's connection.
 */
const SESSION_SETUP = [
   "ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE",
   "ALTER SESSION SET TIMEZONE = 'UTC'",
   "ALTER SESSION SET WEEK_START = 7",
   "ALTER SESSION SET TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DDTHH24:MI:SS.FF3TZH:TZM'",
   "ALTER SESSION SET TIMESTAMP_LTZ_OUTPUT_FORMAT='YYYY-MM-DDTHH24:MI:SS.FF3TZH:TZM'",
];

const jobSecret = process.env.PUBLISHER_QUERY_JOB_SECRET;
if (!jobSecret) {
   logger.warn(
      "PUBLISHER_QUERY_JOB_SECRET is unset: query job ids are signed with a development key",
   );
}
const JOB_SECRET = jobSecret || "development-only-query-job-secret";

function signature(payload: string): Buffer {
   return crypto.createHmac("sha256", JOB_SECRET).update(payload).digest();
}

export function encodeQueryJobId(claims: QueryJobClaims): string {
   const payload = Buffer.from(JSON.stringify(claims)).toString("base64url");
   return `${payload}.${signature(payload).toString("base64url")}`;
}

/** The claims of `jobId`, when it is genuine and was issued for this model. */
export function decodeQueryJobId(
   jobId: string,
   expected: { environment: string; package: string; model: string },
): QueryJobClaims {
   const notFound = new QueryJobNotFoundError("No such query job.");
   const [payload, sig, extra] = jobId.split(".");
   if (!payload || !sig || extra !== undefined) throw notFound;
   const given = Buffer.from(sig, "base64url");
   const wanted = signature(payload);
   if (
      given.length !== wanted.length ||
      !crypto.timingSafeEqual(given, wanted)
   ) {
      throw notFound;
   }
   const claims = JSON.parse(
      Buffer.from(payload, "base64url").toString("utf8"),
   ) as QueryJobClaims;
   if (
      claims.environment !== expected.environment ||
      claims.package !== expected.package ||
      claims.model !== expected.model
   ) {
      throw notFound;
   }
   return claims;
}

/**
 * How one column's values are sent, by Malloy's own row-data rules: an integer
 * as a number when exact and as its digits when not (what a `bigint` cell
 * carries), a date or timestamp as ISO text. The SDK hands integers back as
 * wrapper objects that would otherwise serialize as strings.
 */
export function valueConverter(
   column: snowflake.Column,
): (value: unknown) => unknown {
   const nullable =
      (f: (value: unknown) => unknown) =>
      (value: unknown): unknown =>
         value === null || value === undefined ? null : f(value);
   if (column.isNumber()) {
      return nullable((value) => {
         const n = API.rowDataToNumber(value);
         if (column.getScale() > 0 || Number.isSafeInteger(n)) return n;
         return API.rowDataToSerializedBigint(value);
      });
   }
   if (column.isDate() || column.isTimestamp()) {
      return nullable((value) => API.rowDataToDate(value).toISOString());
   }
   return (value) => value;
}

function execute(
   conn: snowflake.Connection,
   sqlText: string,
   binds?: snowflake.Binds,
): Promise<unknown[] | undefined> {
   return new Promise((resolve, reject) =>
      conn.execute({
         sqlText,
         binds,
         complete: (err, _stmt, rows) => (err ? reject(err) : resolve(rows)),
      }),
   );
}

export class SnowflakeQueryJobClient {
   private session?: Promise<snowflake.Connection>;

   constructor(
      readonly fingerprint: string,
      private readonly config: SnowflakeConnectionConfig,
   ) {}

   private connection(): Promise<snowflake.Connection> {
      this.session ??= this.open().catch((error) => {
         this.session = undefined;
         throw error;
      });
      return this.session;
   }

   private async open(): Promise<snowflake.Connection> {
      const c = this.config;
      const auth = c.privateKey
         ? {
              authenticator: "SNOWFLAKE_JWT",
              privateKey: normalizeSnowflakePrivateKey(c.privateKey),
              ...(c.privateKeyPass ? { privateKeyPass: c.privateKeyPass } : {}),
           }
         : { password: c.password ?? undefined };
      const conn = snowflake.createConnection({
         account: c.account ?? "",
         username: c.username ?? "",
         ...auth,
         warehouse: c.warehouse,
         database: c.database ?? undefined,
         schema: c.schema ?? undefined,
         role: c.role ?? undefined,
         // As Malloy's connector: a session that heartbeats does not expire,
         // and this one owns every job it submitted.
         clientSessionKeepAlive: true,
         clientSessionKeepAliveHeartbeatFrequency: 900,
         jsTreatIntegerAsBigInt: true,
      });
      await new Promise<void>((resolve, reject) =>
         conn.connect((err) => (err ? reject(err) : resolve())),
      );
      for (const sql of SESSION_SETUP) await execute(conn, sql);
      // Snowflake enforces the connection's timeout on the job itself, so a job
      // has a deadline even when nobody is polling it.
      if (c.responseTimeoutMilliseconds && c.responseTimeoutMilliseconds > 0) {
         await execute(
            conn,
            `ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = ${Math.ceil(c.responseTimeoutMilliseconds / 1000)}`,
         );
      }
      return conn;
   }

   /** Submits `sqlText` without waiting for it to run; resolves with Snowflake's query id. */
   async submit(sqlText: string, queryTag?: string): Promise<string> {
      const conn = await this.connection();
      return new Promise((resolve, reject) =>
         conn.execute({
            sqlText,
            asyncExec: true,
            ...(queryTag ? { parameters: { QUERY_TAG: queryTag } } : {}),
            complete: (err, stmt) =>
               err ? reject(err) : resolve(stmt.getQueryId()),
         }),
      );
   }

   async status(queryId: string): Promise<QueryJobStatus> {
      const conn = await this.connection();
      const snowflakeStatus = String(await conn.getQueryStatus(queryId));
      if (conn.isStillRunning(snowflakeStatus as snowflake.QueryStatus)) {
         return { state: "running", snowflakeStatus };
      }
      if (snowflakeStatus === "SUCCESS") {
         return { state: "succeeded", snowflakeStatus };
      }
      try {
         await conn.getQueryStatusThrowIfError(queryId);
      } catch (error) {
         const e = error as {
            code?: string | number;
            message?: string;
            data?: { data?: { queries?: { errorCode?: string | number }[] } };
         };
         // The SDK's own `code` is -1 when the response carries none; the
         // statement's Snowflake error code is on the response it attaches.
         const queryCode = e.data?.data?.queries?.[0]?.errorCode;
         const code =
            queryCode !== undefined
               ? String(queryCode)
               : e.code === undefined
                 ? undefined
                 : String(e.code);
         return {
            // Only an explicit cancel: a statement timeout is a failure.
            state: code === CANCELLED_CODE ? "cancelled" : "failed",
            snowflakeStatus,
            error: { code, message: e.message ?? String(error) },
         };
      }
      return { state: "failed", snowflakeStatus };
   }

   /** A finished job's result: its size, its columns, and a reader for one row range. */
   async result(queryId: string): Promise<{
      totalRows: number;
      columns: string[];
      rows: (
         start: number,
         end: number,
      ) => AsyncIterable<Record<string, unknown>>;
   }> {
      const conn = await this.connection();
      // The promise settles once the status check passes; the result's row
      // count and columns arrive later, with `complete`.
      const stmt = await new Promise<snowflake.RowStatement>(
         (resolve, reject) => {
            conn
               .getResultsFromQueryId({
                  queryId,
                  sqlText: "",
                  streamResult: true,
                  complete: (err, statement) =>
                     err
                        ? reject(err)
                        : resolve(statement as snowflake.RowStatement),
               })
               .catch(reject);
         },
      );
      const columns = stmt.getColumns() ?? [];
      const convert = columns.map(valueConverter);
      return {
         totalRows: stmt.getNumRows(),
         columns: columns.map((column) => column.getName()),
         // Iterated with `for await`, which pulls: the SDK fetches only the
         // result chunks that overlap the range, and no faster than they are read.
         rows: async function* (start, end) {
            for await (const row of stmt.streamRows({
               start,
               end,
            }) as AsyncIterable<Record<string, unknown>>) {
               const out: Record<string, unknown> = {};
               columns.forEach((column, i) => {
                  out[column.getName()] = convert[i](row[column.getName()]);
               });
               yield out;
            }
         },
      };
   }

   async cancel(queryId: string): Promise<void> {
      const conn = await this.connection();
      await execute(conn, "SELECT SYSTEM$CANCEL_QUERY(?)", [queryId]);
   }
}

const clients = new Map<string, SnowflakeQueryJobClient>();

/** The job client for one environment's connection, replaced when its config changes. */
export function snowflakeQueryJobClient(
   environmentName: string,
   connectionName: string,
   config: SnowflakeConnectionConfig,
): SnowflakeQueryJobClient {
   const key = `${environmentName}\u0000${connectionName}`;
   const fingerprint = crypto
      .createHash("sha256")
      .update(JSON.stringify(config))
      .digest("hex");
   let client = clients.get(key);
   if (!client || client.fingerprint !== fingerprint) {
      // The replaced client's session is left open: jobs it submitted may
      // still be running.
      client = new SnowflakeQueryJobClient(fingerprint, config);
      clients.set(key, client);
   }
   return client;
}

/**
 * A Malloy connection whose `runSQL` submits the statement as a job and
 * answers with no rows, for `Malloy.run` to wrap as usual. Only `runSQL` is
 * called on this path.
 */
export function submittingConnection(
   connectionName: string,
   client: SnowflakeQueryJobClient,
   onSubmitted: (queryId: string, sql: string) => void,
): Connection {
   const connection = {
      name: connectionName,
      dialectName: "snowflake",
      async runSQL(
         sql: string,
         options?: RunSQLOptions,
      ): Promise<MalloyQueryData> {
         const bag = options?.queryMetadata;
         const tag =
            bag && Object.keys(bag).length > 0
               ? JSON.stringify(bag)
               : undefined;
         onSubmitted(await client.submit(sql, tag), sql);
         return { rows: [], totalRows: 0 };
      },
   };
   return connection as unknown as Connection;
}
