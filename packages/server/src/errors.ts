// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyError, type LogMessage } from "@malloydata/malloy";
import { PUBLISHER_CONFIG_NAME } from "./constants";
import { logger } from "./logger";
import type { EligibilityRefusalReason } from "./materialization_metrics";
import {
   causeChain,
   isConnectionFailure,
   isCredentialRejection,
} from "./driver_errors";
import { redactPgSecrets } from "./pg_helpers";

export { isConnectionFailure, isCredentialRejection };

// Client-facing bodies for internal failures. The specific message can carry a
// filesystem path, an SQL fragment or an upstream host, so it is logged
// server-side and NOT returned.
//
// Generalizing is decided per branch, not by status class: every 4xx returns
// its message (it names what the caller must change) except the 424 for
// rejected credentials, whose driver text can name a user or account; 501, 503
// and 504 return server-composed messages, though the worker-pool 503s
// interpolate the underlying failure. A NEW 5xx branch must generalize if its
// message comes from a driver, a worker, or the filesystem.
//
// A refused filesystem access (EACCES, EPERM, EROFS) is the exception: only
// the operator can fix it, so its body names the path, composed from the
// errno's fields and never copied from the message.
//
// No error body carries a correlation id yet; adding one belongs server-wide
// (in the Error schema's `details`), not to this branch alone.
const GENERIC_INTERNAL_MESSAGE = "Internal server error.";
const GENERIC_UPSTREAM_MESSAGE = "Upstream connection error.";
const GENERIC_UNREACHABLE_MESSAGE =
   "The database connection is down: the database could not be reached, so the query did not run.";
const GENERIC_AUTH_MESSAGE =
   "The database rejected the connection's credentials. Check the connection's user, password, key or token.";

/**
 * Cap on the logged detail. The message is unbounded and caller-influenced (a
 * driver error can echo the caller's whole SQL), and a log sink with a max line
 * size drops the largest lines first -- the ones worth keeping.
 */
const MAX_LOGGED_DETAIL_CHARS = 2000;

/**
 * Log an internal failure's detail server-side, where the response stops
 * carrying it.
 *
 * - Fields are copied out because `message`/`stack` are non-enumerable, so
 *   logging the Error itself serializes to `{"error":{}}`.
 * - They are nested under `error` because a top-level `message` is winston's
 *   reserved key and would be fused into the summary, making it unique per
 *   failure and useless for grouping or alerting.
 * - Control characters, NEL and U+2028/U+2029 are stripped so caller text
 *   cannot forge log lines; `format.json()` does not escape the latter three.
 * - Postgres passwords and URI userinfo are redacted: a failed DuckLake attach
 *   echoes the metadata connection string, password included.
 * - `level` is `warn` for upstream failures, which a caller can drive in a loop,
 *   so they do not flood the error log meant for our own bugs.
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
   // V8 prefixes the stack with `Name: message`, which is already logged; drop
   // it so a long message does not spend the whole cap and leave no frames.
   const framesOnly = stack.startsWith(`${error.name}: ${message}`)
      ? stack.slice(`${error.name}: ${message}`.length).replace(/^\r?\n/, "")
      : stack;
   logger[level](summary, {
      error: {
         name: error.name,
         message: redactPgSecrets(strip(message)).slice(
            0,
            MAX_LOGGED_DETAIL_CHARS,
         ),
         stack: redactPgSecrets(strip(framesOnly)).slice(
            0,
            MAX_LOGGED_DETAIL_CHARS,
         ),
      },
   });
}

/**
 * Machine-readable discriminator on an error response, for callers that branch
 * on which error they got rather than on prose. The router retries a plain 404
 * on another worker; TABLE_NOT_FOUND tells it not to. Absent means no special
 * handling.
 */
export type ErrorReason =
   | "TABLE_NOT_FOUND"
   // 502: the database could not be reached; the query and model are fine.
   | "CONNECTION_FAILED"
   // 424: the database rejected the connection's credentials.
   | "CONNECTION_AUTH_FAILED"
   // 424: the model names a connection the environment does not have.
   | "CONNECTION_NOT_FOUND";

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
 * The error to answer with when a failure is about the connection rather than
 * the statement: 502 for an unreachable database, 424 for rejected credentials
 * or a missing connection. Undefined otherwise, keeping the route's status.
 * Credentials win when an error matches both.
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
 * Postgres class-42 SQLSTATEs that only the caller's SELECT can raise, so the
 * statement, not the connection, is wrong.
 *
 * An allowlist rather than the whole class: 42501 (insufficient_privilege, also
 * raised at session startup), the generic 42000, and DDL rejections (raised by
 * admin-authored setupSQL) are not the caller's fault and must stay a 502 that
 * pages.
 */
const POSTGRES_STATEMENT_SQLSTATES = new Set([
   "42601", // syntax_error
   "42703", // undefined_column
   "42P01", // undefined_table
   "42883", // undefined_function
   "42704", // undefined_object: a type in a cast
   "42P02", // undefined_parameter
   "42702", // ambiguous_column
   "42725", // ambiguous_function
   "42P09", // ambiguous_alias
   "42P10", // invalid_column_reference
   "42804", // datatype_mismatch
   "42846", // cannot_coerce
   "42803", // grouping_error
   "42P20", // windowing_error
   "42P18", // indeterminate_datatype
   "42P19", // invalid_recursion
   "42712", // duplicate_alias
   "42701", // duplicate_column: CREATE TEMPORARY TABLE AS with two `a`s
   "42602", // invalid_name
   "42622", // name_too_long
   "42809", // wrong_object_type
   "42P21", // collation_mismatch
   "42P22", // indeterminate_collation
]);
const POSTGRES_UNDEFINED_TABLE = "42P01";

/**
 * The one wording of 42P01 that means a table is physically absent. Postgres
 * raises the same code for `missing FROM-clause entry for table "x"` -- an
 * alias the statement never declared, with every real table present -- and
 * answering that 404 TABLE_NOT_FOUND would tell the router a table is gone.
 */
const POSTGRES_RELATION_MISSING = /^relation ".*" does not exist$/;

/**
 * The error to answer with when Postgres rejected the caller's statement:
 * 404 TABLE_NOT_FOUND for a missing table (so the router does not retry it
 * elsewhere), otherwise 400 with the driver's message, which names only the
 * caller's own identifiers. Undefined for anything else.
 *
 * Code and message come from the same error in the `cause` chain, since a
 * wrapper's text was never vetted. FATAL/PANIC is skipped: it ends the
 * session and is never raised by the caller's SELECT.
 *
 * Postgres only: other drivers carry SQLSTATE elsewhere (Snowflake's
 * `sqlState`, whose 42S02 also means "not authorized") or lose it (MySQL).
 */
export function statementRejection(
   error: unknown,
): InvalidArgumentError | TableNotFoundError | undefined {
   for (const current of causeChain(error)) {
      const { code, severity } = current as Error & {
         code?: unknown;
         severity?: unknown;
      };
      if (severity === "FATAL" || severity === "PANIC") return undefined;
      if (typeof code !== "string" || !POSTGRES_STATEMENT_SQLSTATES.has(code)) {
         continue;
      }
      const message = current.message;
      return code === POSTGRES_UNDEFINED_TABLE &&
         POSTGRES_RELATION_MISSING.test(message)
         ? new TableNotFoundError(message)
         : new InvalidArgumentError(message);
   }
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
 * response, and its log line, to the route handler. `onLogged` is called when
 * the mapping logged the error itself, so a route logs only what it did not.
 */
export function internalErrorToHttpError(
   error: Error,
   { log = true, onLogged }: { log?: boolean; onLogged?: () => void } = {},
) {
   const logInternal: typeof logInternalFailure = (...args) => {
      if (!log) return;
      logInternalFailure(...args);
      onLogged?.();
   };
   const access = filesystemAccessFailure(error);
   if (access) {
      // Must precede the typed branches: a PackageNotFoundError wrapping an
      // EACCES would otherwise answer 404 and hide which path is unreadable.
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
   } else if (error instanceof ModelNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof DashboardNotFoundError) {
      return httpError(404, error.message);
   } else if (
      error instanceof SkillNotFoundError ||
      error instanceof AgentNotFoundError
   ) {
      return httpError(404, error.message);
   } else if (error instanceof ConflictError) {
      return httpError(409, error.message);
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
   } else if (error instanceof StorageDestinationNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof StorageCatalogNotFoundError) {
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
      // A server-authored message (callerSafe) is returned as-is; a driver
      // message is logged and generalized, since it can name an internal host,
      // echo SQL, or act as a failure-mode oracle. This also hides a statement
      // the warehouse rejected (for every dialect but Postgres, see
      // statementRejection), because ConnectionError does not distinguish a
      // rejected statement from a transport failure.
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
   } else if (error instanceof WriteRollbackFailedError) {
      return httpError(500, error.message);
   } else if (error instanceof WriteRolledBackError) {
      // Logged once, where the rollback happened, with the cause.
      return httpError(500, error.message);
   } else if (error instanceof ServiceUnavailableError) {
      return httpError(503, error.message);
   } else if (error instanceof PayloadTooLargeError) {
      return httpError(413, error.message);
   } else if (error instanceof QueryTimeoutError) {
      return httpError(504, error.message);
   } else if (error instanceof NotImplementedError) {
      // A feature the server does not have (e.g. `versionId`) is not an
      // internal failure; the OpenAPI spec documents 501 on those routes.
      return httpError(501, error.message);
   } else {
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
         // Omitted rather than undefined, so reason-less bodies have no key.
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
 * A specific argument was malformed; the message says which and what shape was
 * expected. A subclass so MCP advice does not offer Malloy syntax guidance for
 * a non-Malloy argument error. Still a 400.
 */
export class InvalidArgumentError extends BadRequestError {}

/**
 * A dashboard write was refused because the text does not compile; the message
 * names each problem with its line and column. A subclass so telemetry counts
 * a Malloy mistake apart from an API misuse without parsing message text.
 * Still a 400.
 */
export class CompileRefusedError extends BadRequestError {}

/**
 * A model path the write path refuses: unsafe as written, the root
 * `index.malloy`, in `public/`, or leading (through a link) outside the
 * package. A subclass so the write metric counts it as `invalid_path` wherever
 * it is raised; still a 400.
 */
export class InvalidModelPathError extends BadRequestError {}

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
 * `lookup` carries the environment names that do exist, so an MCP tool can
 * suggest one. They stay out of the message because it is also the REST 404
 * body, and a REST caller may be scoped to one environment by the router.
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

/**
 * No skill with that name is in force for the package. Covers both a name
 * nothing defines and a reference file (`<skill>/<stem>`) that no served skill
 * carries.
 */
export class SkillNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/** No agent with that name is served for the package, whether undeclared or dropped by validation. */
export class AgentNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/** A request that names more than one thing where it must name one. 409. */
export class ConflictError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "ConflictError";
   }
}

export class ConnectionNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/**
 * The connection is reachable and authenticated but holds no table at that
 * path: the caller's bad reference, so 404. Distinct from
 * {@link ConnectionError} (502) because a 5xx counts against the router's
 * error budget and pages on-call for a typo in someone's model.
 */
export class TableNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ConnectionError extends Error {
   /**
    * True when {@link message} was authored by this server and is safe to
    * return; false when it carries a driver message verbatim, which can name
    * an internal host or echo SQL. Only the throw site knows which, so it
    * decides. Defaults to false so an unmarked throw site leaks nothing.
    */
   readonly callerSafe: boolean;

   constructor(message: string, options?: { callerSafe?: boolean }) {
      super(message);
      this.callerSafe = options?.callerSafe ?? false;
   }
}

/**
 * The database could not be reached (refused, reset, timed out or closed), so
 * the query never ran: 502 with `reason: CONNECTION_FAILED`. Not 503, which
 * makes a router rerun the query elsewhere against the same database. Raised
 * where {@link isConnectionFailure} matched; the driver message is logged and
 * generalized because it can name an internal host and port.
 */
export class ConnectionFailedError extends ConnectionError {}

/**
 * A model names a connection the environment does not have, usually one
 * deleted after the package loaded: 424 with `reason: CONNECTION_NOT_FOUND`.
 * Distinct from {@link ConnectionNotFoundError}, the 404 for a name the caller
 * typed into a connection route.
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
 * A build or serve path named a storage destination the environment does not
 * configure. 422 rather than 404 because the destination is not the resource
 * being addressed; see {@link StorageDestinationNotFoundError} for that case.
 */
export class DestinationNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/** The storage destination a request's path addresses is not configured: 404. */
export class StorageDestinationNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/**
 * A DuckLake destination's catalog has never been created (nothing built into
 * it yet): 404. The message is composed here, never copied from DuckDB's,
 * which names the metadata connection string.
 */
export class StorageCatalogNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

/**
 * The database rejected the connection's credentials. 424 with
 * `reason: CONNECTION_AUTH_FAILED`: the fix is the connection's configuration,
 * not the query and not a retry.
 */
export class ConnectionAuthError extends Error {
   constructor(message: string) {
      super(message);
   }
}

// A reachable catalog whose on-disk format the pinned DuckLake extension cannot
// attach (see ducklake_version.ts). 422, so it does not read as a credentials
// problem.
export class UnsupportedCatalogFormatError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ModelCompilationError extends Error {
   // Any message-bearing object, so callers can add context and keep the 424.
   constructor(error: { message: string }) {
      super(error.message);
   }
}

/**
 * The package's publisher.json cannot be used as written, so the package is not
 * served. 424, like a model that does not compile. The worker flags it
 * `isManifestError` so the pool restores the class instead of reporting the
 * author's typo as a 503 outage.
 */
export class PackageManifestError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "PackageManifestError";
   }
}

/**
 * A persist source cannot materialize into a `storage=` destination: an
 * unbound free parameter, a given reference (an RLAC/tenant-isolation
 * refusal), or a shape that does not compile in DuckDB. 422, a hard refusal,
 * never a silent fallback; a distinct class so the givens refusal is greppable
 * for security review. `reason` matches what the throw site passed to
 * `recordEligibilityRefused`.
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
 * A chained `storage=` build depends on a persisted upstream it has no table
 * for (not built this run, not supplied, or in another destination).
 * Recomputing it from raw would rebuild a stored table nobody asked for, so
 * under `strictUpstreams` this is refused outright, while a shape failure
 * ({@link MaterializationEligibilityError}) falls back.
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
 * A chained `storage=` build had every stored upstream, but the model assembled
 * over them did not compile: a limit of what the build can carry, not of the
 * source. Recomputing from raw would rebuild the handed-in tables, so under
 * `strictUpstreams` it is refused like a missing upstream.
 */
export class ChainedShapeNotCarriedError extends Error {
   constructor(detail: string) {
      super(detail);
      this.name = "ChainedShapeNotCarriedError";
   }
}

/**
 * The config file exists but cannot be read (malformed JSON, a rejected shape,
 * an unset `${VAR}`). Unlike an absent file, which falls back to the bundled
 * default, this must not degrade to serving nothing while reporting healthy.
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
 * A request refused access to a source (403): an unsatisfied `#(authorize)`,
 * or a gate that could not apply at all. An `#(access_filter)` matching no row
 * is not this; that is a 200 with empty rows.
 */
export class AccessDeniedError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "AccessDeniedError";
   }
}

/**
 * Caller-submitted query text that did not compile, with problem ranges in the
 * text as the caller sent it. Extends MalloyError so class-based compile
 * classification still applies.
 */
export class QueryCompileError extends MalloyError {
   constructor(message: string, problems: LogMessage[]) {
      super(message, problems);
      this.name = "QueryCompileError";
   }
}

/**
 * A problem as the query surface returns it: the `/compile` shape minus the
 * document URL, which for query text is a per-request identifier.
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
 * A query targeted a source or model outside the package's queryable surface
 * under `queryableSources: "declared"`. 404, not 403: the boundary is
 * identity-free. The generic message reads the same for a hidden target as a
 * missing one, so a gated model offers no existence oracle; ungated models use
 * {@link OffSurfaceError}, which says why.
 */
export class NotQueryableError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "NotQueryableError";
   }
}

/**
 * A query-boundary refusal that says why: the target is real but off the
 * published surface, and the message names the fix. Only thrown when the model
 * carries no `#(authorize)` or `#(access_filter)` anywhere, since curation is
 * not access control and there is nothing to hide. Still a 404.
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
 * A write that was applied and then rolled back because the package would not
 * serve it: 500, the server's failure. The underlying failure is the `cause`
 * and is logged, never put in the message, since it can carry a path.
 */
export class WriteRolledBackError extends Error {
   constructor(message: string, options?: ErrorOptions) {
      super(message, options);
      this.name = "WriteRolledBackError";
   }
}

/**
 * A write whose reload failed, and whose restore failed too: the package may
 * serve the new text or none. 500, because only an operator can settle it.
 */
export class WriteRollbackFailedError extends Error {
   constructor(message: string, options?: ErrorOptions) {
      super(message, options);
      this.name = "WriteRollbackFailedError";
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

/** Temporary refusal (e.g. to stay under `PUBLISHER_MAX_MEMORY_BYTES`): 503, retry with back-off. */
export class ServiceUnavailableError extends Error {
   constructor(message: string, options?: ErrorOptions) {
      super(message, options);
   }
}

/**
 * The memory governor refused to admit a new compiled copy of a package: a
 * 503 the caller answers by placing the package elsewhere, not a fault.
 */
export class PackageAdmissionRefusedError extends ServiceUnavailableError {}

/**
 * A response would exceed a server-side size cap (e.g.
 * `PUBLISHER_MAX_QUERY_ROWS`): 413, refine the query or raise the cap, not retry.
 */
export class PayloadTooLargeError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "PayloadTooLargeError";
   }
}

/**
 * A response that could not be serialized at all. Still 413, but distinct so
 * callers are not told to raise a cap: no cap makes it serialize.
 */
export class ResponseUnserializableError extends PayloadTooLargeError {
   constructor(message: string) {
      super(message);
      // Explicit, so a bundler that mangles class names cannot make it log as its parent.
      this.name = "ResponseUnserializableError";
   }
}

/**
 * A query exceeded `PUBLISHER_QUERY_TIMEOUT_MS` and was aborted: 504, so
 * clients can tell "this query is too expensive" from a retryable 503.
 */
export class QueryTimeoutError extends Error {
   constructor(message: string) {
      super(message);
   }
}
