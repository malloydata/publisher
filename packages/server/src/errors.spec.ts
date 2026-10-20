// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import { describe, expect, it } from "bun:test";
import * as net from "net";
import {
   AccessDeniedError,
   AgentNotFoundError,
   BadRequestError,
   ConnectionAuthError,
   ConnectionError,
   ConnectionFailedError,
   ConnectionPoolExhaustedError,
   InvalidArgumentError,
   databaseAccessFailure,
   isConnectionFailure,
   isCredentialRejection,
   statementRejection,
   UnconfiguredConnectionError,
   TableNotFoundError,
   internalErrorToHttpError,
   ModelCompilationError,
   NotImplementedError,
   NotQueryableError,
   PayloadTooLargeError,
   QueryCompileError,
   ResponseUnserializableError,
   QueryTimeoutError,
   ServiceUnavailableError,
   SkillNotFoundError,
   TableAccessDeniedError,
} from "./errors";

describe("internalErrorToHttpError", () => {
   it("maps ConnectionAuthError to 424 with a fixed message and no driver text", () => {
      const { status, json } = internalErrorToHttpError(
         new ConnectionAuthError(
            'password authentication failed for user "analytics"',
         ),
      );
      expect(status).toBe(424);
      expect(json).toEqual({
         code: 424,
         message:
            "The database rejected the connection's credentials. Check the connection's user, password, key or token.",
         reason: "CONNECTION_AUTH_FAILED",
      });
   });

   it("maps SkillNotFoundError and AgentNotFoundError to 404", () => {
      for (const error of [
         new SkillNotFoundError("no skill"),
         new AgentNotFoundError("no agent"),
      ]) {
         const { status, json } = internalErrorToHttpError(error);
         expect(status).toBe(404);
         expect(json).toEqual({ code: 404, message: error.message });
      }
   });

   it("maps BadRequestError to 400", () => {
      const { status, json } = internalErrorToHttpError(
         new BadRequestError("bad input"),
      );
      expect(status).toBe(400);
      expect(json).toEqual({ code: 400, message: "bad input" });
   });

   it("maps AccessDeniedError to 403 (authorize gate)", () => {
      const { status, json } = internalErrorToHttpError(
         new AccessDeniedError('Access denied for source "gated".'),
      );
      expect(status).toBe(403);
      expect(json).toEqual({
         code: 403,
         message: 'Access denied for source "gated".',
      });
   });

   it("maps NotQueryableError to 404 (explore boundary)", () => {
      const { status, json } = internalErrorToHttpError(
         new NotQueryableError('No queryable source "hidden".'),
      );
      expect(status).toBe(404);
      expect(json).toEqual({
         code: 404,
         message: 'No queryable source "hidden".',
      });
   });

   it("maps QueryCompileError to 400 carrying its problems, without the document URL", () => {
      const range = {
         start: { line: 1, character: 28 },
         end: { line: 1, character: 35 },
      };
      const { status, json } = internalErrorToHttpError(
         new QueryCompileError("line 2:29 Unknown function 'coutn'.", [
            {
               message: "Unknown function 'coutn'.",
               severity: "error",
               code: "function-not-found",
               data: null,
               at: { url: "internal://query/0f1e", range },
            },
            // A problem with no location (one the server's own addition to
            // the text produced) keeps its message and gains no `at`.
            {
               message: "'org_id' is not defined",
               severity: "error",
               code: "field-not-found",
            },
         ]),
      );
      expect(status).toBe(400);
      expect(json).toEqual({
         code: 400,
         message: "line 2:29 Unknown function 'coutn'.",
         problems: [
            {
               message: "Unknown function 'coutn'.",
               severity: "error",
               code: "function-not-found",
               at: { range },
            },
            {
               message: "'org_id' is not defined",
               severity: "error",
               code: "field-not-found",
            },
         ],
      });
   });

   it("maps ModelCompilationError to 424", () => {
      const { status, json } = internalErrorToHttpError(
         new ModelCompilationError({ message: "compile failed" }),
      );
      expect(status).toBe(424);
      expect(json).toEqual({ code: 424, message: "compile failed" });
   });

   it("maps TableNotFoundError to 404 with a machine-readable reason", () => {
      const { status, json } = internalErrorToHttpError(
         new TableNotFoundError("Not found: Table proj:ds.missing"),
      );
      expect(status).toBe(404);
      expect(json).toEqual({
         code: 404,
         message: "Not found: Table proj:ds.missing",
         reason: "TABLE_NOT_FOUND",
      });
   });

   it("maps InvalidArgumentError to 400", () => {
      const { status, json } = internalErrorToHttpError(
         new InvalidArgumentError("Improper table path: sal"),
      );
      expect(status).toBe(400);
      expect(json).toEqual({ code: 400, message: "Improper table path: sal" });
   });

   it("omits reason entirely on errors that carry none", () => {
      const { json } = internalErrorToHttpError(
         new ConnectionError("upstream broken"),
      );
      expect(json).not.toHaveProperty("reason");
   });

   it("maps ConnectionError to 502 (distinct from auth, still retryable) with a generic body", () => {
      const { status, json } = internalErrorToHttpError(
         new ConnectionError("upstream broken"),
      );
      expect(status).toBe(502);
      // The driver/connection detail is logged server-side, not echoed to the
      // client (a 502 message can name the internal host or leak a driver oracle).
      expect(json.code).toBe(502);
      expect(json.message).not.toContain("upstream broken");
   });

   it("maps ConnectionPoolExhaustedError to 502 with its own message", () => {
      const message =
         "Connection 'pg' has no free database session: this server opens at most 5 at a time for it, and none came free within 30 s. Retry once fewer queries are running on this connection.";
      const { status, json } = internalErrorToHttpError(
         new ConnectionPoolExhaustedError(message),
      );
      expect(status).toBe(502);
      expect(json).toEqual({ code: 502, message });
   });

   it("falls through to 500 for unrecognized errors with a generic body", () => {
      const { status, json } = internalErrorToHttpError(new Error("boom"));
      expect(status).toBe(500);
      // An unrecognized internal error's message can carry a stack/path/SQL
      // fragment, so it is logged server-side and the client gets a generic body.
      expect(json.code).toBe(500);
      expect(json.message).not.toContain("boom");
   });

   it("maps PayloadTooLargeError to 413", () => {
      const { status, json } = internalErrorToHttpError(
         new PayloadTooLargeError(
            "Query returned more than 100000 rows; refine the query or raise PUBLISHER_MAX_QUERY_ROWS.",
         ),
      );
      expect(status).toBe(413);
      expect(json).toEqual({
         code: 413,
         message:
            "Query returned more than 100000 rows; refine the query or raise PUBLISHER_MAX_QUERY_ROWS.",
      });
   });

   it("maps ResponseUnserializableError to 413 as well, by inheritance", () => {
      // The subclass exists only so the MCP surface can drop the "raise the
      // cap" suggestion; REST must keep answering 413, not fall through to 500.
      const { status, json } = internalErrorToHttpError(
         new ResponseUnserializableError(
            "Query response could not be serialized: the 25356-row result is too large to turn into JSON (byte cap: 50000000). Project fewer columns, add a LIMIT, or filter wide values.",
         ),
      );
      expect(status).toBe(413);
      expect(json.code).toBe(413);
   });

   it("names both payload-size classes, so logs can tell them apart", () => {
      // A subclass that logs as its parent defeats the point of having one.
      expect(new PayloadTooLargeError("x").name).toBe("PayloadTooLargeError");
      expect(new ResponseUnserializableError("x").name).toBe(
         "ResponseUnserializableError",
      );
   });

   it("maps ServiceUnavailableError to 503 (load shedding / back-pressure)", () => {
      const { status, json } = internalErrorToHttpError(
         new ServiceUnavailableError(
            "Pod at max concurrent queries (32); retry later.",
         ),
      );
      expect(status).toBe(503);
      expect(json).toEqual({
         code: 503,
         message: "Pod at max concurrent queries (32); retry later.",
      });
   });

   it("maps NotImplementedError to 501, not the 500 default", () => {
      // The only thrower is the versionId guard, and every route declaring that
      // parameter documents 501. Without a branch here it fell through to 500,
      // reporting an unbuilt feature as an internal failure.
      const { status, json } = internalErrorToHttpError(
         new NotImplementedError("Version IDs not implemented."),
      );
      expect(status).toBe(501);
      expect(json).toEqual({
         code: 501,
         message: "Version IDs not implemented.",
      });
   });

   it("maps QueryTimeoutError to 504 (gateway timeout, distinct from 503 back-pressure)", () => {
      const { status, json } = internalErrorToHttpError(
         new QueryTimeoutError(
            "Query exceeded PUBLISHER_QUERY_TIMEOUT_MS (300000ms) and was aborted.",
         ),
      );
      expect(status).toBe(504);
      expect(json).toEqual({
         code: 504,
         message:
            "Query exceeded PUBLISHER_QUERY_TIMEOUT_MS (300000ms) and was aborted.",
      });
   });
});

describe("connection failure vs a rejected query", () => {
   it("maps ConnectionFailedError to 502 with reason CONNECTION_FAILED and no driver text", () => {
      const { status, json } = internalErrorToHttpError(
         new ConnectionFailedError("connect ECONNREFUSED 10.0.0.5:5432"),
      );
      expect(status).toBe(502);
      expect(json).toEqual({
         code: 502,
         message:
            "The database connection is down: the database could not be reached, so the query did not run.",
         reason: "CONNECTION_FAILED",
      });
   });

   it("maps UnconfiguredConnectionError to 424 with reason CONNECTION_NOT_FOUND", () => {
      const { status, json } = internalErrorToHttpError(
         new UnconfiguredConnectionError("bq_demo"),
      );
      expect(status).toBe(424);
      expect(json).toEqual({
         code: 424,
         message: 'No connection named "bq_demo" found in config',
         reason: "CONNECTION_NOT_FOUND",
      });
   });

   it("keeps a plain ConnectionError's 502 free of a reason", () => {
      // A statement the warehouse rejected on the sqlQuery route is also a
      // ConnectionError. It must not claim the database was unreachable.
      const { json } = internalErrorToHttpError(
         new ConnectionError("syntax error at or near SELEC"),
      );
      expect(json).toEqual({
         code: 502,
         message: "Upstream connection error.",
      });
   });
});

/** A port on loopback that nothing listens on. */
async function closedPort(): Promise<number> {
   const server = net.createServer();
   await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", () => resolve()),
   );
   const { port } = server.address() as net.AddressInfo;
   await new Promise((resolve) => server.close(resolve));
   return port;
}

describe("isConnectionFailure", () => {
   it("recognizes the error Malloy's Postgres driver raises for a closed port", async () => {
      // The real error, not a hand-built one: what reaches Publisher is
      // whatever @malloydata/db-postgres and node-pg actually throw.
      const connection = new PostgresConnection({
         name: "pg",
         host: "127.0.0.1",
         port: await closedPort(),
         username: "nobody",
         databaseName: "nothing",
      });
      const error = await connection.runSQL("SELECT 1").then(
         () => undefined,
         (e: unknown) => e,
      );
      await connection.close();
      expect(error).toBeInstanceOf(Error);
      expect(isConnectionFailure(error)).toBe(true);
   });

   it("recognizes Node's own socket error", async () => {
      const port = await closedPort();
      const error = await new Promise<unknown>((resolve) => {
         const socket = net.connect(port, "127.0.0.1");
         socket.on("error", resolve);
      });
      expect((error as NodeJS.ErrnoException).code).toBe("ECONNREFUSED");
      expect(isConnectionFailure(error)).toBe(true);
   });

   it("follows the cause chain, as Snowflake and Malloy's MySQL driver wrap it", () => {
      const socket = Object.assign(
         new Error("connect ETIMEDOUT 10.0.0.5:443"),
         {
            code: "ETIMEDOUT",
         },
      );
      const wrapped = new Error("Network error. Could not reach Snowflake.", {
         cause: socket,
      });
      expect(isConnectionFailure(wrapped)).toBe(true);
   });

   it("recognizes tedious's unreachable-server codes, as mssql rewraps them", () => {
      // tedious raises a refused or cut TCP connect as ESOCKET, a connect
      // timeout as ETIMEOUT and a failed SQL Browser lookup as EINSTLOOKUP;
      // the mssql pool copies the code onto its own ConnectionError.
      for (const code of ["ESOCKET", "ETIMEOUT", "EINSTLOOKUP"]) {
         const error = Object.assign(
            new Error("Failed to connect to db.example:1433 in 15000ms"),
            { code, name: "ConnectionError" },
         );
         expect(isConnectionFailure(error)).toBe(true);
         expect(isCredentialRejection(error)).toBe(false);
      }
      // A connection cut while a query ran reaches the caller as mssql's
      // RequestError, still carrying ESOCKET.
      expect(
         isConnectionFailure(
            Object.assign(new Error("Connection lost - read ECONNRESET"), {
               code: "ESOCKET",
               name: "RequestError",
            }),
         ),
      ).toBe(true);
   });

   it("does not read a SQL Server request timeout as a server that is down", () => {
      // tedious's ETIMEOUT on a RequestError is a query that outran
      // requestTimeoutMs; the server was reached and ran it. Answering it as
      // unreachable would tell the caller to retry the slow query.
      const error = Object.assign(
         new Error("Timeout: Request failed to complete in 600000ms"),
         { code: "ETIMEOUT", name: "RequestError" },
      );
      expect(isConnectionFailure(error)).toBe(false);
      expect(databaseAccessFailure(error)).toBeUndefined();
   });

   /**
    * The chain the driver produces, as measured under Bun against a SQL
    * Server with a self-signed certificate: mssql's ConnectionError carries
    * tedious's code and keeps tedious's error on `originalError`, not
    * `cause`; tedious's error carries Node's TLS error on `cause`.
    */
   function mssqlWrapped(
      code: string,
      message: string,
      inner?: Error,
      name = "ConnectionError",
   ): Error {
      const tedious = Object.assign(new Error(message, { cause: inner }), {
         code,
      });
      const mssql = Object.assign(new Error(message), { code, name });
      Object.defineProperty(mssql, "originalError", {
         enumerable: true,
         value: tedious,
      });
      return mssql;
   }

   it("does not read a SQL Server certificate the client cannot verify as a server that is down", () => {
      // With encrypt on and trustServerCertificate off, a self-signed
      // certificate fails the TLS handshake; tedious words it as an ESOCKET
      // whose cause is Node's TLS error. A retry cannot fix it.
      const tls = Object.assign(new Error("self signed certificate"), {
         code: "DEPTH_ZERO_SELF_SIGNED_CERT",
      });
      const error = mssqlWrapped(
         "ESOCKET",
         "Failed to connect to db.example:1433 - self signed certificate",
         tls,
      );
      // The outer error has no `cause`: the walk has to follow originalError
      // to find the certificate, which is what this pins.
      expect(error.cause).toBeUndefined();
      expect(isConnectionFailure(error)).toBe(false);
      expect(databaseAccessFailure(error)).toBeUndefined();
   });

   it("still reads an mssql-wrapped refused connect as unreachable", () => {
      const refused = Object.assign(
         new Error("connect ECONNREFUSED 127.0.0.1:1"),
         { code: "ECONNREFUSED" },
      );
      expect(
         isConnectionFailure(
            mssqlWrapped(
               "ESOCKET",
               "Failed to connect to 127.0.0.1:1 - Could not connect (sequence)",
               refused,
            ),
         ),
      ).toBe(true);
   });

   it("recognizes tedious's unreachable wording when only the text survives", () => {
      // The SQL Server connector's table lookup returns a failure as text with
      // the code dropped.
      for (const message of [
         "Failed to connect to 127.0.0.1:1 - Could not connect (sequence)",
         "Failed to connect to db.example:1433 in 15000ms",
         "Failed to connect to db.example:1433 (redirected from gw.example:1433) - connect ECONNRESET",
      ]) {
         expect(isConnectionFailure(new Error(message))).toBe(true);
      }
      // A certificate failure in the same wording is config, not unreachable.
      for (const message of [
         "Failed to connect to db.example:1433 - self signed certificate",
         "Failed to connect to db.example:1433 - unable to verify the first certificate",
         "Failed to connect to db.example:1433 - Hostname/IP does not match certificate's altnames: IP: 10.0.0.5 is not in the cert's list: ",
      ]) {
         expect(isConnectionFailure(new Error(message))).toBe(false);
      }
   });

   it("recognizes a Postgres connection SQLSTATE and a server shutdown", () => {
      // node-pg puts the SQLSTATE in `code`.
      for (const code of ["08006", "08001", "57P01"]) {
         const error = Object.assign(new Error("server gone"), { code });
         expect(isConnectionFailure(error)).toBe(true);
      }
   });

   it("does not read a mysql2 handshake config fault as a connection failure, though it is fatal", () => {
      // mysql2 marks every handshake error fatal. These fail the same way on
      // every retry, so they are not the 502 that says a retry can succeed.
      for (const code of [
         "ER_BAD_DB_ERROR",
         "ER_NOT_SUPPORTED_AUTH_MODE",
         "AUTH_SWITCH_PLUGIN_ERROR",
         "MYSQL_CLEAR_PASSWORD_NOT_ENABLED",
         "HANDSHAKE_NO_SSL_SUPPORT",
         "HANDSHAKE_SSL_ERROR",
      ]) {
         const error = Object.assign(new Error("handshake failed"), {
            code,
            fatal: true,
         });
         expect(isConnectionFailure(error)).toBe(false);
      }
   });

   it("still reads a transient fatal mysql2 error as a connection failure", () => {
      // Too many connections, or a server shutting down: a retry can succeed.
      for (const code of ["ER_CON_COUNT_ERROR", "ER_SERVER_SHUTDOWN"]) {
         const error = Object.assign(new Error("handshake failed"), {
            code,
            fatal: true,
         });
         expect(isConnectionFailure(error)).toBe(true);
      }
   });

   it("reads a transient DNS failure as a connection failure, but not a host that does not resolve", () => {
      const transient = Object.assign(
         new Error("getaddrinfo EAI_AGAIN db.internal"),
         { code: "EAI_AGAIN" },
      );
      expect(isConnectionFailure(transient)).toBe(true);
      // ENOTFOUND is almost always a wrong host in the config: a retry fails
      // the same way. Checked by code and by the code-less message alike.
      const wrongHost = Object.assign(
         new Error("getaddrinfo ENOTFOUND db.internal"),
         { code: "ENOTFOUND" },
      );
      expect(isConnectionFailure(wrongHost)).toBe(false);
      expect(
         isConnectionFailure(
            new Error("Error: getaddrinfo ENOTFOUND db.internal"),
         ),
      ).toBe(false);
   });

   it("recognizes mysql2's fatal flag on a bare driver error", () => {
      // mysql2 sets `fatal: true` on the closed-state error and gives it no
      // code (lib/base/connection.js, _addCommandClosedState).
      const driver = Object.assign(
         new Error("Can't add new command when connection is in closed state"),
         { fatal: true },
      );
      expect(isConnectionFailure(driver)).toBe(true);
   });

   it("recognizes a lost MySQL connection after Malloy's driver drops its code", () => {
      // @malloydata/db-mysql rethrows a query error as `new Error(e)`: the text
      // survives, prefixed "Error: ", and `code` and `fatal` do not. The first
      // two messages are what it threw against MySQL 8.4 for a connection
      // KILLed between queries and during one; the third is a socket error,
      // which Node words as `<syscall> <CODE>`.
      for (const driverMessage of [
         "Can't add new command when connection is in closed state",
         "Connection lost: The server closed the connection.",
         "read ECONNRESET",
      ]) {
         const wrapped = new Error(String(new Error(driverMessage)));
         expect(wrapped.message).toBe(`Error: ${driverMessage}`);
         expect(isConnectionFailure(wrapped)).toBe(true);
      }
   });

   it("recognizes node-pg's codeless lost connection", () => {
      expect(
         isConnectionFailure(new Error("Connection terminated unexpectedly")),
      ).toBe(true);
   });

   it("does not take a query the database rejected for a connection failure", async () => {
      // A real DuckDB rejection: what a bad query actually throws.
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const error = await duckdb.runSQL("SELECT CAST('x' AS INTEGER)").then(
         () => undefined,
         (e: unknown) => e,
      );
      await duckdb.close();
      expect(error).toBeInstanceOf(Error);
      expect(isConnectionFailure(error)).toBe(false);
   });

   it("does not match a Postgres SQLSTATE outside the connection classes", () => {
      // 22012 division_by_zero, 42P01 undefined_table, 28P01 bad password.
      for (const code of ["22012", "42P01", "28P01"]) {
         const error = Object.assign(new Error("rejected"), { code });
         expect(isConnectionFailure(error)).toBe(false);
      }
   });

   it("does not match a codeless message that only quotes a signature", () => {
      // A row value or literal echoed back inside a longer message.
      for (const message of [
         'invalid input syntax for type integer: "Connection terminated unexpectedly"',
         "Table 'connect ECONNREFUSED' does not exist",
         "Unknown field econnrefused in output space",
         "Query execution failed: Error: Connection lost: The server closed the connection.",
         "Error: Unknown column 'read ECONNRESET' in 'field list'",
      ]) {
         expect(isConnectionFailure(new Error(message))).toBe(false);
      }
   });

   it("is false for a thrown non-Error", () => {
      expect(isConnectionFailure("connect ECONNREFUSED")).toBe(false);
      expect(isConnectionFailure(undefined)).toBe(false);
   });
});

describe("isCredentialRejection", () => {
   // Each shape is what the driver raised when measured: Postgres 16 and
   // MySQL 8.4 with a wrong password, through Publisher's connection builder.
   const postgres = Object.assign(
      new Error('password authentication failed for user "postgres"'),
      { code: "28P01" },
   );
   const mysql = Object.assign(
      new Error(
         "Access denied for user 'root'@'192.168.215.1' (using password: YES)",
      ),
      { code: "ER_ACCESS_DENIED_ERROR", fatal: true },
   );

   // tedious: every login the server refuses is an ELOGIN carrying the
   // server's own message; a wrong password is its 18456.
   // As the driver produces it: mssql's ConnectionError copies the code and
   // keeps tedious's error on originalError.
   const sqlserver = Object.assign(new Error("Login failed for user 'sa'."), {
      code: "ELOGIN",
      name: "ConnectionError",
   });
   Object.defineProperty(sqlserver, "originalError", {
      enumerable: true,
      value: Object.assign(new Error("Login failed for user 'sa'."), {
         code: "ELOGIN",
      }),
   });

   it("recognizes a rejected login from Postgres, MySQL, SQL Server, Snowflake and BigQuery", () => {
      expect(isCredentialRejection(postgres)).toBe(true);
      expect(isCredentialRejection(mysql)).toBe(true);
      expect(isCredentialRejection(sqlserver)).toBe(true);
      expect(isConnectionFailure(sqlserver)).toBe(false);
      expect(databaseAccessFailure(sqlserver)).toBeInstanceOf(
         ConnectionAuthError,
      );
      // Snowflake's key-pair failure, as staging logged it, with its code.
      expect(
         isCredentialRejection(
            Object.assign(new Error("JWT token is invalid."), {
               code: "390144",
            }),
         ),
      ).toBe(true);
      const bigquery = new Error("invalid_grant");
      bigquery.name = "BigQueryAuthenticationError";
      expect(isCredentialRejection(bigquery)).toBe(true);
   });

   it("follows the cause chain that schema listing wraps it in", () => {
      // db_utils rethrows "Failed to get schemas ..." with the driver error as
      // cause; the Snowflake 500 on staging was this shape without the cause.
      const wrapped = new Error(
         "Failed to get schemas for Snowflake connection sftest: JWT token is invalid.",
         {
            cause: Object.assign(new Error("JWT token is invalid."), {
               code: "390144",
            }),
         },
      );
      expect(isCredentialRejection(wrapped)).toBe(true);
   });

   it("is not a connection failure's job: a MySQL login is fatal but answers as credentials", () => {
      // Fatal, but its ER_ code names a config fault, not a server that is down.
      expect(isConnectionFailure(mysql)).toBe(false);
      expect(databaseAccessFailure(mysql)).toBeInstanceOf(ConnectionAuthError);
      expect(databaseAccessFailure(postgres)).toBeInstanceOf(
         ConnectionAuthError,
      );
   });

   it("leaves a statement-level permission error alone", () => {
      // 42501 insufficient_privilege: logged in fine, not allowed to read the
      // table. That is the query's problem, not the connection's.
      const denied = Object.assign(new Error("permission denied for table x"), {
         code: "42501",
      });
      expect(isCredentialRejection(denied)).toBe(false);
      expect(databaseAccessFailure(denied)).toBeUndefined();
   });
});

describe("statementRejection", () => {
   // The shape node-pg raises: a DatabaseError whose `code` is the SQLSTATE.
   // Message verbatim from prod 2026-10-13, when a customer dropped a column
   // their model's sql source still selected and the recompile loop answered
   // 502 once a minute until the router paged.
   const column = Object.assign(
      new Error('column "sundance_name" does not exist'),
      { code: "42703" },
   );

   it("answers a statement Postgres rejected as a 400 carrying the driver's message", () => {
      const classified = statementRejection(column);
      expect(classified).toBeInstanceOf(InvalidArgumentError);
      expect(internalErrorToHttpError(classified!)).toEqual({
         status: 400,
         json: { code: 400, message: 'column "sundance_name" does not exist' },
      });
   });

   it("covers the codes a SELECT the caller wrote can produce, not one code", () => {
      for (const [code, message] of [
         ["42601", 'syntax error at or near "FORM"'],
         ["42883", "function nope(integer) does not exist"],
         ["42702", 'column reference "id" is ambiguous'],
         ["42804", "argument of WHERE must be type boolean, not type integer"],
         ["42846", "cannot cast type bytea to integer"],
         ["42803", 'column "s.kind" must appear in the GROUP BY clause'],
         ["42704", 'type "intger" does not exist'],
         ["42701", 'column "a" specified more than once'],
      ]) {
         expect(
            statementRejection(Object.assign(new Error(message), { code })),
         ).toBeInstanceOf(InvalidArgumentError);
      }
   });

   it("leaves 42501 insufficient_privilege where it was: the grants are the connection's, not the statement's", () => {
      // A role without CONNECT fails session startup with 42501 before any
      // caller SQL runs, and a table the connection's user may not read is
      // fixed by a GRANT the caller cannot issue. Both answered 502 before
      // and still do, so a connection that stopped working keeps paging.
      for (const error of [
         Object.assign(new Error('permission denied for database "pw"'), {
            code: "42501",
            severity: "FATAL",
         }),
         Object.assign(new Error("permission denied for table spaces"), {
            code: "42501",
            severity: "ERROR",
         }),
      ]) {
         expect(statementRejection(error)).toBeUndefined();
      }
   });

   it("skips anything Postgres marks FATAL, whatever the code", () => {
      // FATAL ends the session; it is raised at startup or by the server,
      // never by the caller's SELECT, so the code on it is not the
      // statement's.
      expect(
         statementRejection(
            Object.assign(new Error("terminating connection"), {
               code: "42601",
               severity: "FATAL",
            }),
         ),
      ).toBeUndefined();
   });

   it("leaves the class-42 codes a SELECT cannot produce alone", () => {
      // DDL rejections (a duplicate table, an invalid definition) and the
      // generic 42000: an admin's setupSQL could raise these on the same
      // client before the caller's statement, so they stay a fault.
      for (const code of ["42000", "42P07", "42710", "42P16"]) {
         expect(
            statementRejection(
               Object.assign(new Error("some DDL rejection"), { code }),
            ),
         ).toBeUndefined();
      }
   });

   it("answers a table the statement names but the database lacks as TABLE_NOT_FOUND", () => {
      // The router retries a plain 404 on another worker; the reason stops it.
      const missing = Object.assign(
         new Error('relation "public.spaces" does not exist'),
         { code: "42P01" },
      );
      const classified = statementRejection(missing);
      expect(classified).toBeInstanceOf(TableNotFoundError);
      expect(internalErrorToHttpError(classified!).status).toBe(404);
   });

   it("answers a 42P01 whose relation exists -- a missing FROM-clause alias -- as a 400, not a missing table", () => {
      // Postgres raises undefined_table for `SELECT x.id FROM spaces AS s`
      // too. A 404 with TABLE_NOT_FOUND would tell the router a physical
      // table is absent; only the "relation ... does not exist" wording
      // means that.
      const alias = Object.assign(
         new Error('missing FROM-clause entry for table "x"'),
         { code: "42P01" },
      );
      const classified = statementRejection(alias);
      expect(classified).toBeInstanceOf(InvalidArgumentError);
      expect(classified).not.toBeInstanceOf(TableNotFoundError);
      expect(internalErrorToHttpError(classified!).status).toBe(400);
   });

   it("follows the cause chain a wrapper puts it behind, and keeps Postgres's sentence", () => {
      // The 400 carries the message of the error that carried the SQLSTATE,
      // not the wrapper's: "Failed to run SQL" tells the caller nothing, and
      // the wrapper's text was never vetted to name nothing internal.
      const wrapped = new Error("Failed to run SQL", { cause: column });
      expect(statementRejection(wrapped)).toBeInstanceOf(InvalidArgumentError);
      expect(statementRejection(wrapped)!.message).toBe(
         'column "sundance_name" does not exist',
      );
   });

   it("leaves the connection's failures, and anything without a SQLSTATE, alone", () => {
      // Login (class 28) and transport (class 08) are the connection's, and
      // are answered by databaseAccessFailure. A codeless error and a code that
      // is not a SQLSTATE at all say nothing about the statement.
      for (const error of [
         Object.assign(new Error("password authentication failed"), {
            code: "28P01",
         }),
         Object.assign(new Error("terminating connection"), {
            code: "08006",
         }),
         Object.assign(new Error("connect ECONNREFUSED"), {
            code: "ECONNREFUSED",
         }),
         new Error('column "x" does not exist'),
         "a thrown string",
      ]) {
         expect(statementRejection(error)).toBeUndefined();
      }
   });
});

describe("databaseAccessFailure on a table the connection may not read", () => {
   // Verbatim from staging 2026-10-20, a BigQuery connection whose service
   // account had no grant on the dataset a model named. The query path (the
   // job behind sqlQuery, and the dry run behind sqlSource) and the table
   // lookup (tables.get) word it differently; both name the table and nothing
   // else, and both answered 502 "Upstream connection error." with the
   // sentence only in the worker's log.
   const queryDenial =
      "Access Denied: Table ms2-dev-469118:billing_export_dev.gcp_billing_export_resource_v1: User does not have permission to query table ms2-dev-469118:billing_export_dev.gcp_billing_export_resource_v1, or perhaps it does not exist.";
   const lookupDenial =
      "Access Denied: Table ms2-dev-469118:billing_export_dev.gcp_billing_export_resource_v1: Permission bigquery.tables.get denied on table ms2-dev-469118:billing_export_dev.gcp_billing_export_resource_v1 (or it may not exist).";
   // As @google-cloud/common raises it from createQueryJob: an ApiError with
   // the HTTP status in `code` and BigQuery's own reason in `errors`.
   const apiError = Object.assign(new Error(queryDenial), {
      code: 403,
      errors: [
         { domain: "global", reason: "accessDenied", message: queryDenial },
      ],
   });

   it("answers 424 TABLE_ACCESS_DENIED carrying BigQuery's sentence", () => {
      // The grant is the connection's, as with rejected credentials (424), so
      // it is neither the caller's 400 nor a 502 that retries on another
      // worker and pages for a customer's IAM.
      const classified = databaseAccessFailure(apiError);
      expect(classified).toBeInstanceOf(TableAccessDeniedError);
      expect(isCredentialRejection(apiError)).toBe(false);
      expect(internalErrorToHttpError(classified!, { log: false })).toEqual({
         status: 424,
         json: {
            code: 424,
            message: queryDenial,
            reason: "TABLE_ACCESS_DENIED",
         },
      });
   });

   it("recognizes the table lookup's wording, which the driver returns as text", () => {
      expect(databaseAccessFailure(new Error(lookupDenial))).toBeInstanceOf(
         TableAccessDeniedError,
      );
   });

   it("recognizes the sentence a publisher proxy connection relays without its status", () => {
      // db-publisher rethrows the remote's body message as a plain Error and
      // drops the status, so the text is all the local server receives.
      expect(databaseAccessFailure(new Error(queryDenial))).toBeInstanceOf(
         TableAccessDeniedError,
      );
   });

   it("follows the cause chain and answers with the sentence that matched, not the wrapper's", () => {
      const wrapped = new Error("Failed to run SQL", { cause: apiError });
      const classified = databaseAccessFailure(wrapped);
      expect(classified).toBeInstanceOf(TableAccessDeniedError);
      expect(classified!.message).toBe(queryDenial);
   });

   it("never reads the denial as a missing table, though BigQuery says it may be one", () => {
      // TABLE_NOT_FOUND tells the router the table is gone. BigQuery does not
      // say which it is, and the table in the staging case existed.
      const classified = databaseAccessFailure(new Error(lookupDenial));
      expect(classified).not.toBeInstanceOf(TableNotFoundError);
      expect(internalErrorToHttpError(classified!, { log: false }).status).toBe(
         424,
      );
   });

   it("leaves a project-level refusal where it was: it fails every statement, not one table", () => {
      // A connection that cannot create jobs at all is the connection-wide
      // case, like a Postgres role without CONNECT, and still answers 502.
      expect(
         databaseAccessFailure(
            Object.assign(
               new Error(
                  "Access Denied: Project ms2-staging: User does not have bigquery.jobs.create permission in project ms2-staging.",
               ),
               { code: 403 },
            ),
         ),
      ).toBeUndefined();
   });

   it("matches from the start of the message, so a quoted name cannot", () => {
      for (const message of [
         'Syntax error: Unexpected string literal "Access Denied: Table a:b.c: x" at [1:8]',
         "Permission denied while getting table proj:ds",
         "Access Denied: Table",
      ]) {
         expect(databaseAccessFailure(new Error(message))).toBeUndefined();
      }
   });
});
