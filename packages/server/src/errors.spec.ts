// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import { describe, expect, it } from "bun:test";
import * as net from "net";
import {
   AccessDeniedError,
   BadRequestError,
   ConnectionAuthError,
   ConnectionError,
   ConnectionFailedError,
   ConnectionPoolExhaustedError,
   InvalidArgumentError,
   databaseAccessFailure,
   isConnectionFailure,
   isCredentialRejection,
   UnconfiguredConnectionError,
   TableNotFoundError,
   internalErrorToHttpError,
   ModelCompilationError,
   NotImplementedError,
   NotQueryableError,
   PackageVersionError,
   PayloadTooLargeError,
   QueryCompileError,
   ResponseUnserializableError,
   QueryTimeoutError,
   ServiceUnavailableError,
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

   it("recognizes a rejected login from Postgres, MySQL, Snowflake and BigQuery", () => {
      expect(isCredentialRejection(postgres)).toBe(true);
      expect(isCredentialRejection(mysql)).toBe(true);
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

describe("PackageVersionError", () => {
   it("answers each reason with its status and carries the reason", () => {
      const expected = {
         MANIFEST_VERSION_MISSING: 400,
         MANIFEST_VERSION_INVALID: 400,
         VERSION_ID_INVALID: 400,
         VERSION_NOT_FOUND: 404,
         VERSION_CONFLICT: 409,
         PACKAGE_IS_VERSIONED: 409,
         VERSION_IS_LATEST: 409,
         VERSION_IS_LAST_ACTIVE: 409,
         VERSION_BUILDING: 409,
         VERSION_NOT_LATEST: 400,
         VERSION_ARCHIVED: 410,
      } as const;
      for (const [name, status] of Object.entries(expected)) {
         const reason = name as keyof typeof expected;
         const answer = internalErrorToHttpError(
            new PackageVersionError(reason, `refused: ${reason}`),
            { log: false },
         );
         expect(answer).toEqual({
            status,
            json: { code: status, message: `refused: ${reason}`, reason },
         });
      }
   });
});
