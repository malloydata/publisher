// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyError, type LogMessage } from "@malloydata/malloy";
import { PUBLISHER_CONFIG_NAME } from "./constants";
import { logger } from "./logger";
import type { EligibilityRefusalReason } from "./materialization_metrics";

// Client-facing body for an internal failure (500/502). The specific error
// message can carry internal detail -- a filesystem path, an SQL fragment, an
// upstream host -- so it is logged server-side (below) and NOT returned.
//
// Generalizing is decided per branch, not by status class -- 501, 503 and 504
// are 5xx and still return their messages. Every 4xx returns its message
// because a client error names what the caller must change, with one
// exception: the 424 for rejected credentials returns a fixed message, because
// its driver text can name a user or an account. The 5xx branches
// that return theirs do so because the message is one this server composed (a
// missing feature, a cap that was reached, a timeout), which is true of most of
// them but not all: the worker-pool and compile-worker throws behind 503
// interpolate the underlying failure, so a crash message reaches the caller
// there. An unusable publisher.json does not land in that branch: it throws
// PackageManifestError, which the pool's wire shape carries by class, so it
// maps to 424 below instead of reading as a worker outage.
//
// So a NEW 5xx branch is a decision rather than a default: generalize it here
// if its message comes from a driver, a worker, or the filesystem.
//
// The one filesystem failure that is NOT generalized is a refused access
// (EACCES, EPERM, EROFS; see filesystemAccessFailure). It is a deployment
// fault only the operator can fix, and the generic body hides which mount is
// wrong. Its body is composed here from the errno's code, syscall and path,
// never copied from the error's message.
//
// Neither generic body carries a correlation handle, which is what a user
// reporting "I got Internal server error." would hand an operator to find the
// logged detail. That is deliberately unchanged rather than overlooked: no error
// response in this server has ever carried one (the `details` field the Error
// schema declares is populated nowhere, and the MCP JSON-RPC path answers with a
// bare "Internal server error" too), so adding one only here would make this the
// single exception rather than the new convention. Worth doing server-wide --
// `loggerMiddleware` already derives a W3C traceId when the caller sends
// `traceparent`, and it would go in `details` with no schema change -- but it is
// its own change, and it wants an id that exists for callers who send no
// traceparent.
const GENERIC_INTERNAL_MESSAGE = "Internal server error.";
const GENERIC_UPSTREAM_MESSAGE = "Upstream connection error.";
const GENERIC_UNREACHABLE_MESSAGE =
   "The database connection is down: the database could not be reached, so the query did not run.";
const GENERIC_AUTH_MESSAGE =
   "The database rejected the connection's credentials. Check the connection's user, password, key or token.";

/**
 * Cap on the logged detail. `error.message` here is unbounded and
 * caller-influenced: a MalloyError embeds the whole compile error text, and the
 * sqlQuery path wraps a driver error that can echo the caller's entire SQL
 * statement, bounded only by the request body limit. With a stack appended that
 * is multi-KB per failure, and a log sink with a max line size drops the largest
 * lines first -- precisely the ones worth keeping.
 */
const MAX_LOGGED_DETAIL_CHARS = 2000;

/**
 * Log an internal failure's detail server-side, in the one place the response
 * stops carrying it.
 *
 * The fields are copied out explicitly rather than passing the Error itself:
 * `message` and `stack` are non-enumerable own properties, so
 * `logger.error(msg, { error })` on an actual Error serializes to
 * `{"error":{}}` under both formats this server configures -- the detail would
 * exist nowhere at all. Copying the fields is also what lets the guards below
 * apply to them.
 *
 * They are nested under `error` rather than spread at the top level because
 * `message` is winston's own reserved key: a top-level `message` is fused into
 * `info.message`, so the summary becomes "<summary> <the whole driver error>".
 * That makes the summary unique per failure -- unusable as a grouping key or an
 * alert condition -- and leaves no queryable field holding just the error text.
 * Nesting keeps the summary stable and puts the detail at `error.message`. This
 * is not the `{ error }` bug above returning: these are plain strings, so
 * nothing depends on non-enumerable properties.
 *
 * Newlines and other control characters are stripped because the default format
 * (colorize + simple, whenever OTEL_EXPORTER_OTLP_ENDPOINT is unset) is
 * newline-delimited plain text, so a message carrying `\n` -- and caller SQL can
 * -- could otherwise forge log entries. The range also covers the separators
 * JSON.stringify does NOT escape (NEL, and the U+2028/U+2029 line and paragraph
 * separators): those reach the rendered line verbatim under both formats, so
 * `format.json()` is not a backstop for them the way it is for `\n`.
 *
 * `level` separates the two cases that reach here, because they mean different
 * things to whoever is watching. An unrecognized error is a bug in this server
 * and belongs at `error`. An upstream connection failure is usually the
 * caller's or the warehouse's, and a caller can drive it in a loop with bad
 * SQL, so logging it at `error` lets one client fill the error log and move an
 * error-rate dashboard meant to track our own faults. It goes to `warn`.
 */
export function logInternalFailure(
   summary: string,
   error: Error,
   level: "error" | "warn" = "error",
): void {
   // Strip first, cap second: the caller decides how much of the stripped value
   // to keep, because the stack needs its own budget (below).
   const strip = (value: string): string =>
      // eslint-disable-next-line no-control-regex
      value.replace(/[\u0000-\u001f\u007f-\u009f\u2028\u2029]/g, " ");
   const message = error.message ?? "";
   const stack = error.stack ?? "";
   // V8 prefixes the stack with `Name: message`, so capping the stack from
   // character 0 spends the whole budget on a message that is already logged in
   // its own field -- a 3KB driver error echoing the caller's SQL (the case
   // MAX_LOGGED_DETAIL_CHARS exists for) leaves zero frames, on the branch where
   // the frames are the point. Drop the prefix, then cap what remains.
   const framesOnly = stack.startsWith(`${error.name}: ${message}`)
      ? stack.slice(`${error.name}: ${message}`.length).replace(/^\r?\n/, "")
      : stack;
   logger[level](summary, {
      error: {
         name: error.name,
         message: strip(message).slice(0, MAX_LOGGED_DETAIL_CHARS),
         stack: strip(framesOnly).slice(0, MAX_LOGGED_DETAIL_CHARS),
      },
   });
}

/**
 * Machine-readable discriminator on an error response, for callers that must
 * branch on *which* 404 they got rather than on prose.
 *
 * The router retries a 404 by invalidating its cached worker location and
 * asking the control plane again, because a 404 normally means the worker it
 * called no longer hosts that environment or connection. A table that is not in
 * the database is also a 404 but carries no such implication -- retrying it
 * re-queries the same absent table and throws away a cache entry every other
 * caller on that connection is using. Only reasons that a caller is expected to
 * branch on are emitted; absence is the norm and means "no special handling".
 */
export type ErrorReason =
   | "TABLE_NOT_FOUND"
   // On a 502: the database could not be reached, so the query never ran. The
   // query and the model are fine, and rewriting either will not help. Marks a
   // 502 as the customer's database being down, not Credible failing.
   | "CONNECTION_FAILED"
   // On a 424: the database rejected the connection's credentials (a wrong
   // password, an invalid key, an expired token). The query never ran, and
   // whoever configures the connection has to fix it.
   | "CONNECTION_AUTH_FAILED"
   // On a 424: the model names a connection the environment does not have,
   // usually one deleted after the package was loaded.
   | "CONNECTION_NOT_FOUND"
   | PackageVersionReason;

/**
 * Why a request about a package version was refused. Emitted as `reason`
 * because several share a status, and the difference is what a caller acts on:
 * "bump the version" and "this package's versions are immutable" are both 409.

 */
export type PackageVersionReason =
   | "MANIFEST_VERSION_MISSING"
   | "MANIFEST_VERSION_INVALID"
   | "VERSION_CONFLICT"
   | "PACKAGE_IS_VERSIONED"
   | "VERSION_IS_LATEST"
   | "VERSION_BUILDING"
   | "VERSION_ID_INVALID"
   | "VERSION_NOT_FOUND"
   | "VERSION_ARCHIVED";

const PACKAGE_VERSION_STATUS: Record<PackageVersionReason, number> = {
   MANIFEST_VERSION_MISSING: 400,
   MANIFEST_VERSION_INVALID: 400,
   VERSION_CONFLICT: 409,
   PACKAGE_IS_VERSIONED: 409,
   VERSION_IS_LATEST: 409,
   VERSION_BUILDING: 409,
   VERSION_ID_INVALID: 400,
   VERSION_NOT_FOUND: 404,
   VERSION_ARCHIVED: 410,
};

const FILESYSTEM_ACCESS_DESCRIPTIONS: Record<string, string> = {
   EACCES: "permission denied",
   EPERM: "operation not permitted",
   EROFS: "read-only file system",
};

/**
 * The refused filesystem access behind `error`, if there is one: the error
 * itself or anything on its `cause` chain that is a Node errno error with an
 * access code. Requiring `syscall` keeps a driver error that merely carries a
 * `code` string from matching.
 */
export function filesystemAccessFailure(
   error: unknown,
): NodeJS.ErrnoException | undefined {
   let current: unknown = error;
   for (let depth = 0; depth < 8 && current instanceof Error; depth++) {
      const errno = current as NodeJS.ErrnoException;
      if (
         typeof errno.code === "string" &&
         errno.code in FILESYSTEM_ACCESS_DESCRIPTIONS &&
         typeof errno.syscall === "string"
      ) {
         return errno;
      }
      current = errno.cause;
   }
   return undefined;
}

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
 * Whether `error` means the database could not be reached, as opposed to the
 * database running the statement and rejecting it.
 *
 * Read from the driver's structured fields on the error and its `cause` chain:
 * a Node socket code, a Postgres connection SQLSTATE, or mysql2's `fatal`
 * flag, which it sets when the connection is unusable, unless the same error
 * carries a code naming a config fault ({@link MYSQL_CONFIG_CODES}). Message text is the
 * last resort, for the few drivers that raise a connection failure with no
 * code, and is matched whole so a row value or a table name quoted inside a
 * longer message cannot match.
 */
export function isConnectionFailure(error: unknown): boolean {
   let current: unknown = error;
   for (let depth = 0; depth < 8 && current instanceof Error; depth++) {
      const { code, fatal } = current as Error & {
         code?: unknown;
         fatal?: unknown;
      };
      if (typeof code === "string") {
         if (NODE_CONNECTION_CODES.has(code)) return true;
         if (POSTGRES_CONNECTION_SQLSTATE.test(code)) return true;
      }
      if (fatal === true && !isMysqlConfigFault(code)) return true;
      const message = current.message.trim();
      if (CODELESS_CONNECTION_MESSAGES.some((re) => re.test(message))) {
         return true;
      }
      current = current.cause;
   }
   return false;
}

/** Postgres SQLSTATEs for a rejected login: class 28. */
const POSTGRES_AUTH_SQLSTATE = /^28[0-9A-Z]{3}$/;

/** mysql2's code for a rejected user or password. */
const MYSQL_AUTH_CODES = new Set(["ER_ACCESS_DENIED_ERROR"]);

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
 * SQLSTATE in class 28, mysql2's `ER_ACCESS_DENIED_ERROR`, a Snowflake login
 * code, or the `BigQueryAuthenticationError` Malloy's BigQuery driver raises
 * for a rejected key or token.
 */
export function isCredentialRejection(error: unknown): boolean {
   let current: unknown = error;
   for (let depth = 0; depth < 8 && current instanceof Error; depth++) {
      const { code } = current as Error & { code?: unknown };
      if (typeof code === "string" || typeof code === "number") {
         const value = String(code);
         if (POSTGRES_AUTH_SQLSTATE.test(value)) return true;
         if (MYSQL_AUTH_CODES.has(value)) return true;
         if (SNOWFLAKE_AUTH_CODES.has(value)) return true;
      }
      if (current.name === "BigQueryAuthenticationError") return true;
      current = current.cause;
   }
   return false;
}

/**
 * The error to answer with when a failure is about the connection rather than
 * the statement: rejected credentials, an unreachable database, or a
 * connection the environment does not have. An unreachable database is a 502
 * (something is down); rejected credentials and a missing connection are 424
 * (something is misconfigured). Undefined for anything else, which keeps the
 * status its route gave it.
 *
 * Credentials are checked first, so an error that matches both answers as
 * credentials.
 */
export function databaseAccessFailure(
   error: unknown,
):
   | ConnectionAuthError
   | ConnectionFailedError
   | UnconfiguredConnectionError
   | undefined {
   if (
      error instanceof ConnectionAuthError ||
      error instanceof ConnectionFailedError ||
      error instanceof UnconfiguredConnectionError
   ) {
      return error;
   }
   const message = error instanceof Error ? error.message : String(error);
   if (isCredentialRejection(error)) return new ConnectionAuthError(message);
   if (isConnectionFailure(error)) return new ConnectionFailedError(message);
   return undefined;
}

/**
 * One line naming a refused filesystem access, in Node's own shape
 * (`EACCES: permission denied, mkdir '/x'`), built from the errno's fields.
 */
export function describeFilesystemAccessFailure(
   errno: NodeJS.ErrnoException,
): string {
   const code = errno.code ?? "";
   const target = errno.path ? ` '${errno.path}'` : "";
   return `${code}: ${FILESYSTEM_ACCESS_DESCRIPTIONS[code] ?? code}, ${errno.syscall}${target}`;
}

/**
 * `error.message`, followed by the refused filesystem access that caused it
 * when the message does not already name it. For records that keep only a
 * message, like a /status loadErrors entry.
 */
export function messageWithFilesystemCause(error: unknown): string {
   const message = error instanceof Error ? error.message : String(error);
   const access = filesystemAccessFailure(error);
   if (!access || message.includes(access.code ?? "")) return message;
   return `${message}: ${describeFilesystemAccessFailure(access)}`;
}

/** The errno fields of `error`, for a wire shape that keeps only what it names. */
export function errnoWireFields(
   error: Error,
): { code: string; syscall?: string; path?: string } | undefined {
   const { code, syscall, path } = error as NodeJS.ErrnoException;
   if (typeof code !== "string") return undefined;
   return {
      code,
      ...(typeof syscall === "string" ? { syscall } : {}),
      ...(typeof path === "string" ? { path } : {}),
   };
}

/**
 * Map an error to the HTTP response it answers with. `log: false` classifies
 * without logging, for a caller that only needs the status and leaves the
 * response, and its log line, to the route handler.
 */
export function internalErrorToHttpError(
   error: Error,
   { log = true }: { log?: boolean } = {},
) {
   const logInternal: typeof logInternalFailure = (...args) => {
      if (log) logInternalFailure(...args);
   };
   const access = filesystemAccessFailure(error);
   if (access) {
      // Ahead of the typed branches: a wrap like PackageNotFoundError around
      // an EACCES would otherwise answer 404, which reads as "does not exist"
      // and sends the operator looking for a file that is there. That also
      // means a 4xx class whose cause chain holds a refused access answers 500
      // with the errno; moving this branch below any typed branch demotes the
      // errno to that branch's status and message.
      logInternal("Filesystem access refused", error, "warn");
      return httpError(
         500,
         `The server cannot access a path it needs (${describeFilesystemAccessFailure(access)}). ` +
            `Give the user the server runs as access to it.`,
      );
   }
   if (error instanceof BadRequestError) {
      return httpError(400, error.message);
   } else if (error instanceof ServerConfigurationError) {
      logInternal("Server configuration error", error, "warn");
      return httpError(500, error.message);
   } else if (error instanceof FrozenConfigError) {
      return httpError(403, error.message);
   } else if (error instanceof AccessDeniedError) {
      return httpError(403, error.message);
   } else if (error instanceof EnvironmentNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof PackageNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof PackageVersionError) {
      return httpError(
         PACKAGE_VERSION_STATUS[error.reason],
         error.message,
         error.reason,
      );
   } else if (error instanceof ModelNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof DashboardNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof NotQueryableError) {
      return httpError(404, error.message);
   } else if (error instanceof QueryCompileError) {
      return {
         status: 400,
         json: {
            code: 400,
            message: error.message,
            problems: error.problems.map(toQueryTextProblem),
         },
      };
   } else if (error instanceof MalloyError) {
      return httpError(400, error.message);
   } else if (error instanceof TableNotFoundError) {
      return httpError(404, error.message, "TABLE_NOT_FOUND");
   } else if (error instanceof ConnectionNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof DestinationNotFoundError) {
      return httpError(422, error.message);
   } else if (error instanceof ConnectionAuthError) {
      // The driver's text can name the user, the account or the host, so it
      // is logged and the body says only what to fix. warn: a misconfigured
      // connection is the customer's to fix, not our fault.
      logInternal("Connection credentials rejected", error, "warn");
      return httpError(424, GENERIC_AUTH_MESSAGE, "CONNECTION_AUTH_FAILED");
   } else if (error instanceof ConnectionFailedError) {
      // Checked ahead of ConnectionError, which it extends: the same 502 and
      // the same logging, plus the reason that says the database is down.
      logInternal("Database unreachable", error, "warn");
      return httpError(502, GENERIC_UNREACHABLE_MESSAGE, "CONNECTION_FAILED");
   } else if (error instanceof UnconfiguredConnectionError) {
      return httpError(424, error.message, "CONNECTION_NOT_FOUND");
   } else if (error instanceof UnsupportedCatalogFormatError) {
      return httpError(422, error.message);
   } else if (error instanceof MaterializationEligibilityError) {
      return httpError(422, error.message);
   } else if (error instanceof ModelCompilationError) {
      return httpError(424, error.message);
   } else if (error instanceof PackageManifestError) {
      return httpError(424, error.message);
   } else if (error instanceof ConnectionError) {
      // 502. A server-authored message (see ConnectionError.callerSafe) is
      // actionable and returned as-is; anything wrapping a driver message is
      // logged and generalized, because it can name an internal host/port, echo
      // the caller's SQL, or distinguish refused from timed-out from auth-failed.
      //
      // This intentionally covers a statement the warehouse itself rejected, on
      // the sqlSource and sqlQuery paths, and that is the uncomfortable half of
      // the trade: "object DB.SCHEMA.FOO does not exist" is the most useful
      // sentence the product produces, and only the caller can act on it. It is
      // generalized anyway because ConnectionError is one class covering both a
      // rejected statement and any driver failure no recognizer knows, and the
      // same text that names the caller's own typo names an internal hostname
      // when the failure is ours. (An unreachable database and rejected
      // credentials are recognized, and answered above.) Splitting the class -- a rejected statement as 4xx with its
      // message, transport failure as a generic 502 -- is the right end state
      // and wants its own change; a table path that names nothing already took
      // that route (see TableNotFoundError, 404). Until then a caller who needs
      // the driver's text gets it from the logs, by traceparent.
      if (error.callerSafe) {
         return httpError(502, error.message);
      }
      logInternal("Upstream connection error", error, "warn");
      return httpError(502, GENERIC_UPSTREAM_MESSAGE);
   } else if (error instanceof MaterializationNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof MaterializationConflictError) {
      return httpError(409, error.message);
   } else if (error instanceof InvalidStateTransitionError) {
      return httpError(409, error.message);
   } else if (error instanceof WriteConflictError) {
      return httpError(409, error.message);
   } else if (error instanceof WriteRolledBackError) {
      logInternal("Dashboard write rolled back", error, "warn");
      return httpError(500, error.message);
   } else if (error instanceof ServiceUnavailableError) {
      return httpError(503, error.message);
   } else if (error instanceof PayloadTooLargeError) {
      return httpError(413, error.message);
   } else if (error instanceof QueryTimeoutError) {
      return httpError(504, error.message);
   } else if (error instanceof NotImplementedError) {
      // 501, not the 500 default. Asking for a feature the server does not have
      // (today: a `versionId`, which every route declaring it rejects) is not an
      // internal failure, and the OpenAPI spec has documented 501 on those
      // routes all along.
      return httpError(501, error.message);
   } else {
      // Unrecognized error: a genuine internal failure. Its message may carry a
      // stack fragment, path, or SQL, so log it server-side and return a generic
      // body to the client.
      logInternal("Unhandled internal error", error);
      return httpError(500, GENERIC_INTERNAL_MESSAGE);
   }
}

function httpError(code: number, message: string, reason?: ErrorReason) {
   return {
      status: code,
      json: {
         code,
         message: message,
         // Omitted rather than undefined so existing toStrictEqual assertions
         // on reason-less errors keep passing.
         ...(reason ? { reason } : {}),
      },
   };
}

export class NotImplementedError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class BadRequestError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/**
 * A specific argument was malformed, and the message says which and what shape
 * was expected.
 *
 * A subclass rather than a plain BadRequestError because the two want different
 * agent-facing advice. BadRequestError is this codebase's general wrapper for
 * query-time failures too ("Model compilation failed: ...", filter validation),
 * which are Malloy problems and should keep the Malloy syntax guidance. These
 * are not about Malloy at all: a schema-introspection argument error answered
 * with four suggestions about `source:` and `view:` keywords sends the caller
 * to edit a model they never mentioned.
 *
 * Still a BadRequestError, so it still maps to HTTP 400.
 */
export class InvalidArgumentError extends BadRequestError {}

/**
 * A dashboard write was refused because the text does not compile, and the
 * message names each problem with its line and column.
 *
 * A subclass so telemetry can tell the compile gate doing its job apart from a
 * malformed request — a frozen config, a path that is not a dashboard, a body
 * with no source. Those are a caller getting the API wrong; this one is a
 * caller getting Malloy wrong, and an operator watching the write path needs
 * the two counted separately. Classifying on the message text would have
 * worked until someone reworded it.
 *
 * Still a BadRequestError, so it still maps to HTTP 400.
 */
export class CompileRefusedError extends BadRequestError {}

/** A document's text carries a URL-producing render tag or markup in a label; counted apart from a restricted construct. */
export class RenderTagRefusedError extends CompileRefusedError {}

/** The restricted-construct gate could not parse the text, so it judged nothing: a refusal for a fragment, a plain compile problem for one tile of a document. */
export class UnparseableTextError extends CompileRefusedError {
   constructor(
      message: string,
      /** The parser's own words, which the message alone does not carry. */
      readonly detail: string,
   ) {
      super(message);
   }
}

/**
 * `lookup` is set where a name was looked up and missed. It carries the names
 * that do exist, so an MCP tool can tell the agent what to use instead. The
 * names stay out of the message because the message is also the REST 404 body,
 * and a REST caller may be scoped to one environment by the router in front.
 * The MCP tools do put the names in their message, to any caller. That adds no
 * disclosure: `list_packages` already names every loaded environment over MCP.
 */
export class EnvironmentNotFoundError extends Error {
   constructor(
      message: string,
      readonly lookup?: {
         environmentName: string;
         availableEnvironments: string[];
      },
   ) {
      super(message);
   }
}

/**
 * A request about a package version refused for {@link reason}, which decides
 * its status (404, 409, 410, or 400 for a publish whose manifest version is
 * missing or malformed) and is returned as the response's `reason`.
 */
export class PackageVersionError extends Error {
   constructor(
      readonly reason: PackageVersionReason,
      message: string,
   ) {
      super(message);
      this.name = "PackageVersionError";
   }
}

export class PackageNotFoundError extends Error {
   constructor(message: string, options?: ErrorOptions) {
      super(message, options);
   }
}

export class ModelNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/**
 * No dashboard with that slug in the package. Distinct from
 * {@link ModelNotFoundError}: a `dashboards/*.malloy` with no `# artifact` tag
 * is a shared include, so the file can exist as a model and still not be a
 * dashboard.
 */
export class DashboardNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ConnectionNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/**
 * The connection is reachable and authenticated, but it holds no table at that
 * path. A caller's bad reference, not a server or upstream fault, so it maps to
 * 404 -- which is what every spec declaring this route has always documented
 * (502 appears in none of them).
 *
 * Distinct from {@link ConnectionError}, which stays 502 for upstream failures
 * such as an exhausted quota. (An unreachable database is
 * {@link ConnectionFailedError}, also 502; rejected credentials are
 * {@link ConnectionAuthError}, 424.) The
 * split matters beyond tidiness, because a 5xx here is counted against the
 * router's server-error budget and pages on-call for what is a typo in someone's
 * model.
 */
export class TableNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ConnectionError extends Error {
   /**
    * True when {@link message} was authored by this server and is safe to return
    * to the caller; false (the default) when it carries a driver or upstream
    * error verbatim.
    *
    * The 502 class covers two different things. Some are server-authored and
    * purely actionable -- "Table x.y not found" tells the caller to fix the table
    * name and names nothing internal. The rest wrap a driver message that can
    * carry an internal host/port, the caller's own SQL, or a failure-mode oracle
    * (refused vs timed out vs auth-failed). Genericizing the whole class to
    * suppress the second kind would throw away the first, so the distinction is
    * made where the error is raised, by whoever knows which one it is.
    *
    * Defaults to false so an unmarked message is generalized: a new throw site
    * that forgets to think about this leaks nothing.
    */
   readonly callerSafe: boolean;

   constructor(message: string, options?: { callerSafe?: boolean }) {
      super(message);
      this.callerSafe = options?.callerSafe ?? false;
   }
}

/**
 * The database could not be reached: the connection was refused, reset or
 * timed out, or the server closed it. The query never ran, so it maps to 502
 * with `reason: CONNECTION_FAILED`, not to the 400 a rejected query gets.
 *
 * 5xx because something is down, and a retry can succeed; a misconfigured
 * connection (rejected credentials, a deleted connection) is a 424 instead.
 * 502 is already what this server answers for a driver failure, rather than
 * 500 (our bug), 503 (our overload: a router cools the worker down and reruns
 * the query elsewhere, against the same database) or 504 (our query timeout).
 * The reason is what tells it from Credible failing.
 *
 * Raised only where {@link isConnectionFailure} recognized the driver's error.
 * The message is the driver's, so it is logged and generalized: it can name an
 * internal host and port.
 */
export class ConnectionFailedError extends ConnectionError {}

/**
 * A model names a connection the environment does not have, usually because
 * it was deleted after the package was loaded. Raised by the environment's
 * connection lookup when Malloy's own lookup fails for a name the environment
 * does not configure, with Malloy's message. Maps to 424 with
 * `reason: CONNECTION_NOT_FOUND`.
 *
 * Distinct from {@link ConnectionNotFoundError}, the 404 a connection route
 * answers for a name the caller typed: here the caller named nothing, and
 * nothing a retry elsewhere would fix.
 */
export class UnconfiguredConnectionError extends Error {
   constructor(name: string, options?: { cause?: unknown }) {
      super(`No connection named "${name}" found in config`, options);
   }
}

/**
 * Every database session a connection may open from this process was busy for
 * the whole wait, so the query never reached the database. A 502 like any other
 * connection-side failure, with a server-authored message, so the caller learns
 * the cause instead of the generic upstream text.
 */
export class ConnectionPoolExhaustedError extends ConnectionError {
   constructor(message: string) {
      super(message, { callerSafe: true });
      this.name = "ConnectionPoolExhaustedError";
   }
}

/**
 * A storage destination was named but is not configured on the
 * environment. Distinct from {@link ConnectionNotFoundError} so a misconfigured
 * destination is diagnosable in logs, and mapped to 422 rather than 404 because
 * it can only be raised by a build or serve path: the connection endpoints
 * resolve through the connection list alone, which never holds a destination and
 * so answers for one exactly as it does for a name that does not exist.
 */
export class DestinationNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/**
 * The database rejected the connection's credentials: a wrong password, an
 * invalid key, an expired token. Raised where {@link isCredentialRejection}
 * recognized the driver's error. Maps to 424 with
 * `reason: CONNECTION_AUTH_FAILED`: the query never ran, and the fix is the
 * connection's configuration, not the query and not a retry.
 */
export class ConnectionAuthError extends Error {
   constructor(message: string) {
      super(message);
   }
}

// A catalog was reached and authenticated fine, but its on-disk format is
// outside the range the pinned engine's extension can attach (see
// ducklake_version.ts). Distinct from ConnectionAuthError so the 422 doesn't
// read as a credentials problem. Maps to HTTP 422.
export class UnsupportedCatalogFormatError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ModelCompilationError extends Error {
   // Accepts a MalloyError or any message-bearing object, so callers that add
   // context around a compile failure (e.g. naming the source whose authorize
   // annotation failed) can reuse this 424 mapping without a separate class.
   constructor(error: { message: string }) {
      super(error.message);
   }
}

/**
 * The package's publisher.json cannot be used as written: it is not a JSON
 * object, it has a malformed `explores` or an unknown `scope`, or its two
 * `scope` homes disagree. The
 * package is not served until the author fixes the file.
 *
 * 424, like a model that does not compile: the request was fine, the package it
 * depends on is not. The manifest is read inside the package-load worker, so
 * the worker flags it `isManifestError` and deserializeError restores the
 * class; without that the pool reports the author's typo as a 503 outage.
 */
export class PackageManifestError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "PackageManifestError";
   }
}

/**
 * A persist source was asked to materialize into a `storage=` destination (the
 * DuckDB/DuckLake tier) but is ineligible: it has an unbound free parameter, it
 * references a given (an RLAC/tenant-isolation refusal), or its served shape
 * does not compile in DuckDB. Mapped to HTTP **422** (the request is
 * well-formed, but the source cannot be processed into a materialized artifact)
 * — a hard refuse, never a silent fallback. Kept a distinct class so the
 * givens/RLAC refusal is greppable for security review. Accepts a
 * message-bearing object to match {@link ModelCompilationError}'s ergonomics.
 * `reason` is optional so an existing throw site need not be touched to keep
 * compiling; every current throw site sets it, matching the same value it
 * hands `recordEligibilityRefused` — a caller that needs the bounded reason
 * (rather than parsing the message) reads it off the error instead of a
 * second classification pass.
 */
export class MaterializationEligibilityError extends Error {
   readonly reason?: EligibilityRefusalReason;

   constructor(error: { message: string; reason?: EligibilityRefusalReason }) {
      super(error.message);
      this.name = "MaterializationEligibilityError";
      this.reason = error.reason;
   }
}

/**
 * A chained `storage=` build found that its downstream depends on a persisted
 * source the build cannot see: one that was neither built in this run nor
 * supplied by reference, or one whose table lives in another destination. Not a
 * shape problem — the downstream may well compile over its parents — but a
 * dispatch one: the orchestrator meant to pin that upstream and this build has
 * no table for it. Recomputing it from raw would rebuild a stored table the
 * orchestrator did not ask for, which is the mis-build `strictUpstreams`
 * exists to refuse, so under strict this error is refused outright while a
 * shape failure ({@link MaterializationEligibilityError}) falls back.
 */
export class ChainedUpstreamMissingError extends Error {
   readonly missing: readonly string[];

   constructor(missing: readonly string[], detail: string) {
      super(detail);
      this.name = "ChainedUpstreamMissingError";
      this.missing = missing;
   }
}

/**
 * A chained `storage=` build had every stored upstream it depends on, and the
 * downstream reaches nothing but those, yet the model assembled over them did
 * not compile — a limit of what the build can carry (a refinement not
 * re-emitted, a construct the destination's dialect lacks), not a property of
 * the source. Recomputing from raw WOULD build it, but it would also rebuild
 * stored tables the build was handed, so under `strictUpstreams` this is
 * refused like a missing upstream rather than recomputed like a source that
 * genuinely reaches the warehouse ({@link MaterializationEligibilityError}
 * from the chained path).
 */
export class ChainedShapeNotCarriedError extends Error {
   constructor(detail: string) {
      super(detail);
      this.name = "ChainedShapeNotCarriedError";
   }
}

/**
 * The config file exists but could not be turned into a manifest: malformed
 * JSON, a shape the loader rejects, or a `${VAR}` reference to an unset
 * environment variable.
 *
 * Distinct from the file being ABSENT, which is not an error: Publisher then
 * falls back to the bundled DuckDB-only default. This is a file the operator
 * wrote and Publisher cannot honour, so it must not degrade to serving nothing
 * while reporting healthy.
 */
export class PublisherConfigError extends Error {
   constructor(configName: string, cause: unknown) {
      super(
         `Could not read ${configName}: ${
            cause instanceof Error ? cause.message : String(cause)
         }. Fix the file, or move it aside to fall back to the bundled default.`,
      );
      this.name = "PublisherConfigError";
      this.cause = cause;
   }
}

/**
 * The server is deployed in a way it cannot work with, such as a credentials
 * path that names a directory. HTTP 500 with the message, which this server
 * composes and which tells the operator what to change.
 */
export class ServerConfigurationError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "ServerConfigurationError";
   }
}

export class FrozenConfigError extends Error {
   constructor(
      message = `Publisher config can't be updated when ${PUBLISHER_CONFIG_NAME} has { "frozenConfig": true }`,
   ) {
      super(message);
   }
}

/**
 * A request was refused access to a source (HTTP 403), for one of two reasons:
 * an `#(authorize)` lock the supplied givens do not satisfy, or either route's
 * gate failing to apply at all (an unresolvable shape, nothing to attach to, a
 * referenced given with no value). An `#(access_filter)` that simply matches no
 * row is NOT this — that is a 200 with the caller's (empty) rows.
 */
export class AccessDeniedError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "AccessDeniedError";
   }
}

/**
 * Caller-submitted query text that did not compile. Each problem's range is
 * expressed in the text exactly as the caller sent it, not the text the server
 * compiled, so a client can point at the failing span of its own payload.
 *
 * Extends MalloyError so every consumer that classifies a compile failure by
 * class (the MCP error advice, restricted-mode detection by problem code) keeps
 * treating this as one.
 */
export class QueryCompileError extends MalloyError {
   constructor(message: string, problems: LogMessage[]) {
      super(message, problems);
      this.name = "QueryCompileError";
   }
}

/**
 * A problem as the query surface returns it: the shape `/compile` uses, minus
 * the document URL. The query text has no URL of its own; the one the compiler
 * assigns it is a per-request identifier that names nothing a caller can open.
 */
function toQueryTextProblem(problem: LogMessage) {
   return {
      message: problem.message,
      severity: problem.severity,
      code: problem.code,
      ...(problem.at ? { at: { range: problem.at.range } } : {}),
   };
}

/**
 * A query targeted a source/model that is not part of the package's queryable
 * surface under `queryableSources: "declared"` (a non-`explores` model file, or
 * a source not in a model's `export {}` closure). Mapped to HTTP **404**, not
 * 403: unlike `#(authorize)` (which is identity-scoped and answers "who"), the
 * explore boundary is identity-free and answers "what is queryable". This
 * class carries the generic message, which reads the same for a hidden target
 * as for a missing one, so a gated model offers no enumeration or existence
 * oracle. Where nothing is gated, the refusal is the {@link OffSurfaceError}
 * subclass instead, which says why.
 */
export class NotQueryableError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "NotQueryableError";
   }
}

/**
 * A query-boundary refusal that says why: the target is real, it is off the
 * package's published surface, and the message names that surface and the fix.
 *
 * Only thrown when the model that refused carries no gate, `#(authorize)` or
 * `#(access_filter)`, anywhere. The generic NotQueryableError exists so a hidden GATED source is
 * indistinguishable from a missing one. An ungated hidden source has nothing to
 * protect that way: curation is not access control, and `/compile` (exempt from
 * the boundary) already answers a hidden file differently from a missing one.
 * Without the reason, a modeler who saves a new file and queries it reads the
 * 404 as a typo.
 *
 * Still a NotQueryableError, so it still maps to 404.
 */
export class OffSurfaceError extends NotQueryableError {
   constructor(message: string) {
      super(message);
      this.name = "OffSurfaceError";
   }
}

export class MaterializationNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class MaterializationConflictError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/** A write whose `expectedHash` no longer matches the file: someone else saved first. */
export class WriteConflictError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "WriteConflictError";
   }
}

/**
 * A write that was applied and then taken back: the package would not serve it,
 * so the previous text was put back. The caller's request was well-formed and
 * the file compiled, so this is the server's failure, not theirs — 500, with
 * the message, which says what state the package was left in. The underlying
 * failure is attached as `cause` and logged, never put in the message; it can
 * carry a path. A refused filesystem access in the cause chain still answers
 * through the composed-errno branch of `internalErrorToHttpError`.
 */
export class WriteRolledBackError extends Error {
   constructor(message: string, options?: ErrorOptions) {
      super(message, options);
      this.name = "WriteRolledBackError";
   }
}

/** A refusal from a write's post-reload check, worded for the caller. */
export class WriteVerifyError extends Error {
   constructor(message: string, options?: ErrorOptions) {
      super(message, options);
      this.name = "WriteVerifyError";
   }
}

export class InvalidStateTransitionError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/**
 * Thrown when the publisher is temporarily refusing a request to keep
 * RSS under the configured `PUBLISHER_MAX_MEMORY_BYTES` cap. Mapped to
 * HTTP 503 so an upstream proxy / client can retry with back-off.
 */
export class ServiceUnavailableError extends Error {
   constructor(message: string, options?: ErrorOptions) {
      super(message, options);
   }
}

/**
 * The memory governor refused to admit a new compiled copy of a package. A
 * 503 like any {@link ServiceUnavailableError}, but an answer to one request
 * rather than a fault of the server or the package: the caller places the
 * package elsewhere, and nothing about this server is left to repair.
 */
export class PackageAdmissionRefusedError extends ServiceUnavailableError {}

/**
 * Thrown when a response would exceed a server-side size cap (e.g. an
 * ad-hoc connection SQL query that returned more than
 * `PUBLISHER_MAX_QUERY_ROWS` rows). Mapped to HTTP 413 so callers know
 * the request was well-formed but the result is too large for the
 * publisher to materialize; the remediation is "refine the query" or
 * "raise the cap", not "retry".
 */
export class PayloadTooLargeError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "PayloadTooLargeError";
   }
}

/**
 * The subset of {@link PayloadTooLargeError} where the response could not be
 * serialized at all, rather than merely measuring over the cap. Still HTTP 413
 * by inheritance, because the request was well-formed and the result is too
 * large; the distinction exists so callers are not told to raise a cap. Raising
 * `PUBLISHER_MAX_RESPONSE_BYTES` cannot help here, because there is no cap at
 * which a response that will not serialize starts serializing, so the only
 * remedies are the ones that shrink the response.
 */
export class ResponseUnserializableError extends PayloadTooLargeError {
   constructor(message: string) {
      super(message);
      // Set explicitly rather than derived, so it survives a bundler that
      // mangles class names. Without it the subclass logs as its parent, which
      // defeats the point of a class callers are meant to tell apart.
      this.name = "ResponseUnserializableError";
   }
}

/**
 * Thrown when a query exceeded the configured wall-clock budget
 * (`PUBLISHER_QUERY_TIMEOUT_MS`) and the publisher aborted it
 * mid-execution. Mapped to HTTP 504 (`Gateway Timeout`) because the
 * publisher acts as a gateway to the underlying database — the
 * upstream caller did nothing wrong, but the downstream query took
 * too long. Distinct from {@link ServiceUnavailableError} so clients
 * can distinguish "back off, the pod is loaded" (503, retryable)
 * from "this specific query is too expensive" (504, refine it).
 */
export class QueryTimeoutError extends Error {
   constructor(message: string) {
      super(message);
   }
}
