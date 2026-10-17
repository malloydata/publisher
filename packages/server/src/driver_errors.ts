// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Recognizers for what a database driver's error means, independent of which
 * driver threw it: an unreachable database, or rejected credentials. Matched
 * by error code and name, on the error and its `cause` chain, never by
 * Publisher's own error classes.
 */

/**
 * Node's codes for a socket that could not connect or was cut. Postgres,
 * MySQL and Snowflake all surface these, on the error itself or on its `cause`.
 *
 * `ENOTFOUND` is left out: a host name that does not resolve is almost always
 * a wrong host in the connection's config, which a retry will not fix.
 * `EAI_AGAIN` is the transient DNS failure, and is in.
 */
const NODE_CONNECTION_CODES = new Set([
   "ECONNREFUSED",
   "ECONNRESET",
   "ECONNABORTED",
   "EPIPE",
   "ETIMEDOUT",
   "EAI_AGAIN",
   "EHOSTUNREACH",
   "ENETUNREACH",
]);

/**
 * Postgres SQLSTATEs that mean the session is gone rather than that the
 * statement was wrong: class 08 (connection exception) and the three
 * operator-intervention codes for a server shutting down.
 */
const POSTGRES_CONNECTION_SQLSTATE = /^(08[0-9A-Z]{3}|57P0[123])$/;

/**
 * Messages that arrive with no code, matched from the start of the message so
 * a value quoted later in it cannot match.
 *
 * node-pg raises "Connection terminated unexpectedly" with no code.
 *
 * `@malloydata/db-mysql` wraps every query error in `new Error(e)`, which keeps
 * only the text, prefixed "Error: ", and drops mysql2's `code` and `fatal`. So
 * a MySQL connection lost during or between queries is recognized by mysql2's
 * own wording: a closed connection reused, a connection the server closed, or
 * a Node socket error, which Node words as `<syscall> <CODE>`. A connection
 * refused at connect time keeps its code: the driver connects outside that
 * wrapper.
 */
const CODELESS_CONNECTION_MESSAGES = [
   /^Connection terminated unexpectedly$/,
   /^(Error: )?Can't add new command when connection is in closed state$/,
   /^(Error: )?Connection lost: The server closed the connection\.$/,
   new RegExp(
      `^(Error: )?(connect|read|write|getaddrinfo) (${[
         ...NODE_CONNECTION_CODES,
      ].join("|")})\\b`,
   ),
   // tedious's wording for a SQL Server it could not reach, kept by the
   // SQL Server connector's table lookup, which returns a failure as text with
   // the code dropped: a connect that timed out (`… in 15000ms`) or failed
   // (`… - <reason>`), optionally after an Azure redirect. A reason naming a
   // certificate is excluded: that is a TLS verification failure, a fault in
   // the connection's config rather than a server that is down.
   /^Failed to connect to \S+( \(redirected from \S+\))?( in \d+ms| - (?!.*certificate).+)$/i,
];

/**
 * mysql2 codes that come with `fatal` for a fault in the connection's config,
 * which a retry will not fix. mysql2 marks every handshake and auth-switch
 * error fatal, so `fatal` alone cannot tell these from a server that is down.
 * An explicit list rather than an `ER_` prefix, because some fatal `ER_` codes
 * are transient (`ER_CON_COUNT_ERROR`, too many connections, and
 * `ER_SERVER_SHUTDOWN`) and some config faults have no `ER_` prefix.
 * `ER_ACCESS_DENIED_ERROR` is here so it is never read as unreachable; it
 * answers as rejected credentials.
 */
const MYSQL_CONFIG_CODES = new Set([
   "ER_ACCESS_DENIED_ERROR",
   "ER_BAD_DB_ERROR",
   "ER_NOT_SUPPORTED_AUTH_MODE",
   "AUTH_SWITCH_PLUGIN_ERROR",
   "MYSQL_CLEAR_PASSWORD_NOT_ENABLED",
]);

function isMysqlConfigFault(code: unknown): boolean {
   return (
      typeof code === "string" &&
      (MYSQL_CONFIG_CODES.has(code) || code.startsWith("HANDSHAKE_"))
   );
}

/**
 * tedious's codes for a SQL Server it could not reach: the TCP connect failed
 * or was cut (`ESOCKET`), or the SQL Browser lookup for a named instance
 * failed (`EINSTLOOKUP`). The `mssql` wrapper keeps the code when it rewraps,
 * and a connection cut mid-query reaches the caller as its `RequestError`
 * carrying `ESOCKET`, so these are read off the code alone. `ELOGIN` is
 * deliberately absent: it carries every server-side login refusal, which is a
 * config fault (see {@link isCredentialRejection}).
 */
const SQLSERVER_CONNECTION_CODES = new Set(["ESOCKET", "EINSTLOOKUP"]);

/**
 * tedious's `ETIMEOUT` names two different timeouts: the connect timeout, on
 * its `ConnectionError`, and the request timeout, on its `RequestError`, which
 * is a query that ran longer than `requestTimeoutMs`. Only the first is a
 * server that could not be reached; the second answered as unreachable would
 * tell a caller to retry the slow query. `mssql` rewraps both and names its
 * own classes, so the class name tells them apart. The name is mssql's, not
 * tedious's, so this depends on the connector reaching tedious through mssql.
 */
function isSqlserverConnectTimeout(error: Error, code: string): boolean {
   return code === "ETIMEOUT" && error.name === "ConnectionError";
}

/**
 * Node's TLS verification codes. tedious raises a certificate the client
 * cannot verify as an `ESOCKET` whose `cause` is the TLS error, so without
 * this a self-signed certificate on a server with `encrypt` on would read as
 * a server that is down and answer as retryable. It is a fault in the
 * connection's config (`trustServerCertificate`, `hostNameInCertificate`, or
 * the CA bundle), which a retry will not fix.
 */
const TLS_VERIFICATION_CODES = new Set([
   "CERT_HAS_EXPIRED",
   "DEPTH_ZERO_SELF_SIGNED_CERT",
   "ERR_TLS_CERT_ALTNAME_INVALID",
   "SELF_SIGNED_CERT_IN_CHAIN",
   "UNABLE_TO_GET_ISSUER_CERT",
   "UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
   "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
]);

/**
 * The error `error` wraps: its `cause`, or, for the `mssql` driver, its
 * `originalError`. mssql rewraps tedious's error in its own class and keeps
 * the original there rather than on `cause`, so a walk that read only `cause`
 * would stop at mssql's error and never see what tedious attached beneath it
 * (the TLS failure under an `ESOCKET`, for one).
 */
function wrappedError(error: Error): unknown {
   if (error.cause instanceof Error) return error.cause;
   return (error as Error & { originalError?: unknown }).originalError;
}

function causeChainHasTlsVerificationFailure(error: Error): boolean {
   let current: unknown = wrappedError(error);
   for (let depth = 0; depth < 8 && current instanceof Error; depth++) {
      const { code } = current as Error & { code?: unknown };
      if (typeof code === "string" && TLS_VERIFICATION_CODES.has(code)) {
         return true;
      }
      current = wrappedError(current);
   }
   return false;
}

/**
 * Whether `error` means the database could not be reached, as opposed to the
 * database running the statement and rejecting it.
 *
 * Read from the driver's structured fields on the error and its `cause` chain:
 * a Node socket code, a Postgres connection SQLSTATE, a tedious socket or
 * connect-timeout code (unless it wraps a TLS verification failure, which is
 * config), or mysql2's `fatal` flag unless the same error carries a config-fault
 * code ({@link MYSQL_CONFIG_CODES}). Message text is the last resort, for
 * drivers that raise one with no code, and is matched whole so a value quoted
 * inside a longer message cannot match.
 */
export function isConnectionFailure(error: unknown): boolean {
   for (const current of causeChain(error)) {
      const { code, fatal } = current as Error & {
         code?: unknown;
         fatal?: unknown;
      };
      if (typeof code === "string") {
         if (NODE_CONNECTION_CODES.has(code)) return true;
         if (POSTGRES_CONNECTION_SQLSTATE.test(code)) return true;
         if (
            SQLSERVER_CONNECTION_CODES.has(code) ||
            isSqlserverConnectTimeout(current, code)
         ) {
            return !causeChainHasTlsVerificationFailure(current);
         }
      }
      if (fatal === true && !isMysqlConfigFault(code)) return true;
      const message = current.message.trim();
      if (CODELESS_CONNECTION_MESSAGES.some((re) => re.test(message))) {
         return true;
      }
   }
   return false;
}

/**
 * `error` and the Errors it wraps (see {@link wrappedError}: `cause`, or
 * mssql's `originalError`), outermost first, at most eight deep. The one walk
 * the driver-error recognizers share: a driver's structured fields (`code`,
 * `fatal`, `severity`) sit on whichever error in the chain the driver raised,
 * and a wrapper such as db_utils's "Failed to get schemas ..." puts that one
 * behind a `cause`.
 */
export function* causeChain(error: unknown): Iterable<Error> {
   let current: unknown = error;
   for (let depth = 0; depth < 8 && current instanceof Error; depth++) {
      yield current;
      current = wrappedError(current);
   }
}

/** Postgres SQLSTATEs for a rejected login: class 28. */
const POSTGRES_AUTH_SQLSTATE = /^28[0-9A-Z]{3}$/;

/** mysql2's code for a rejected user or password. */
const MYSQL_AUTH_CODES = new Set(["ER_ACCESS_DENIED_ERROR"]);

/**
 * tedious's code for every login the server refused: a wrong user or password
 * (its 18456), a database the login cannot open (4060), a login not permitted
 * for this kind of connection (18452). The server's own message rides along,
 * so the response says which. It is also what tedious raises when the login
 * exchange ends without an acknowledgement, which is still not a server that
 * is down.
 */
const SQLSERVER_AUTH_CODES = new Set(["ELOGIN"]);

/**
 * Snowflake's login failures, as its server reports them in `code`: a wrong
 * user or password (390100), an invalid key-pair JWT (390144), an invalid ID
 * token (390195), an expired OAuth token (390318). Session-token codes are
 * left out, because the SDK renews those itself.
 */
const SNOWFLAKE_AUTH_CODES = new Set(["390100", "390144", "390195", "390318"]);

/**
 * Whether `error` means the database rejected the connection's credentials.
 *
 * Read from the driver's fields on the error and its `cause` chain: a Postgres
 * SQLSTATE in class 28, mysql2's `ER_ACCESS_DENIED_ERROR`, tedious's `ELOGIN`,
 * a Snowflake login code, or the `BigQueryAuthenticationError` Malloy's
 * BigQuery driver raises for a rejected key or token.
 */
export function isCredentialRejection(error: unknown): boolean {
   for (const current of causeChain(error)) {
      const { code } = current as Error & { code?: unknown };
      if (typeof code === "string" || typeof code === "number") {
         const value = String(code);
         if (POSTGRES_AUTH_SQLSTATE.test(value)) return true;
         if (MYSQL_AUTH_CODES.has(value)) return true;
         if (SQLSERVER_AUTH_CODES.has(value)) return true;
         if (SNOWFLAKE_AUTH_CODES.has(value)) return true;
      }
      if (current.name === "BigQueryAuthenticationError") return true;
   }
   return false;
}
