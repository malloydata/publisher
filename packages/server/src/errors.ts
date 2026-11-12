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
   | "CONNECTION_NOT_FOUND"
   // 409: the package's build plan could not be derived at load; a reload
   // derives it again, a retry does not.
   | "BUILD_PLAN_UNAVAILABLE"
   // 503: the package's build plan is being re-derived after a connection
   // change; retry after the Retry-After seconds.
   | "BUILD_PLAN_REFRESHING"
   // 409: the package is fetched again from the location it was installed
   // from on reload, which would discard the write.
   | "PACKAGE_INSTALLED_FROM_LOCATION"
   // 409: a create-only report write found a file at the path; another name
   // may be free.
   | "FILE_EXISTS"
   // Package versions; each is raised only as a PackageVersionError, whose
   // reason fixes its status (see PACKAGE_VERSION_ERROR_STATUS).
   | PackageVersionErrorReason;

/**
 * Why a package-version request was refused. Several share a status code, and
 * the reason is how a caller tells "bump the version" from "the model is
 * broken" or "move latest first".
 */
export type PackageVersionErrorReason =
   | "MANIFEST_VERSION_MISSING"
   | "MANIFEST_VERSION_INVALID"
   | "VERSION_ID_INVALID"
   | "VERSION_NOT_FOUND"
   | "VERSION_CONFLICT"
   | "PACKAGE_IS_VERSIONED"
   | "VERSION_IS_LATEST"
   | "VERSION_IS_LAST_ACTIVE"
   | "VERSION_BUILDING"
   | "VERSION_NOT_LATEST"
   | "VERSION_ARCHIVED";

const PACKAGE_VERSION_ERROR_STATUS: Record<PackageVersionErrorReason, number> =
   {
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
   };

/** A package-version request refused for `reason`, which decides its status. */
export class PackageVersionError extends Error {
   get kind(): "package_version" {
      return "package_version";
   }

   constructor(
      readonly reason: PackageVersionErrorReason,
      message: string,
   ) {
      super(message);
      this.name = "PackageVersionError";
   }

   get status(): number {
      return PACKAGE_VERSION_ERROR_STATUS[this.reason];
   }
}

/**
 * The checks on deleting a version while other versions remain, given
 * whether it is in service, the package's `latest`, and how many versions are
 * in service. Throws the refusal; returns when the delete may go ahead.
 */
export function refuseVersionDelete(
   packageName: string,
   versionId: string,
   inService: boolean,
   latest: string | null,
   activeCount: number,
): void {
   if (latest === versionId) {
      throw new PackageVersionError(
         "VERSION_IS_LATEST",
         `Version ${versionId} is the latest version of package ${packageName}; make another version latest before deleting it.`,
      );
   }
   if (inService && activeCount <= 1) {
      throw new PackageVersionError(
         "VERSION_IS_LAST_ACTIVE",
         `Version ${versionId} is the last version of package ${packageName} in service; delete its archived versions first, or delete the package.`,
      );
   }
}

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
 * The 4xx status of a request body body-parser refused (too large, malformed
 * JSON, unsupported charset). Its errors are http-errors, not this module's
 * classes: they carry a `type` such as `entity.too.large`, a `status`, and
 * `expose: true` for a message safe to return.
 */
function bodyParserClientStatus(error: Error): number | undefined {
   const e = error as { type?: unknown; status?: unknown; expose?: unknown };
   if (typeof e.type !== "string" || e.expose !== true) return undefined;
   const status = e.status;
   return typeof status === "number" && status >= 400 && status < 500
      ? status
      : undefined;
}

/** What {@link internalErrorToHttpError} answers with. */
export interface HttpErrorAnswer {
   status: number;
   /** Headers to send with it, when the error's mapping names any. */
   headers?: Record<string, string>;
   json: {
      code: number;
      message: string;
      reason?: ErrorReason;
      problems?: ReturnType<typeof toQueryTextProblem>[];
   };
}

/**
 * Map an error to the HTTP response it answers with. `log: false` classifies
 * without logging, for a caller that only needs the status and leaves the
 * response, and its log line, to the route handler. `onLogged` is called when
 * the mapping logged the error itself, so a route logs only what it did not.
 *
 * The answer is the {@link ERROR_TABLE} row for the error's {@link errorKind},
 * after one check no kind expresses: a refused filesystem access anywhere on
 * the `cause` chain.
 */
export function internalErrorToHttpError(
   error: Error,
   { log = true, onLogged }: { log?: boolean; onLogged?: () => void } = {},
): HttpErrorAnswer {
   const logInternal: typeof logInternalFailure = (...args) => {
      if (!log) return;
      logInternalFailure(...args);
      onLogged?.();
   };
   const access = filesystemAccessFailure(error);
   if (access) {
      // Must precede the table: a PackageNotFoundError wrapping an EACCES
      // would otherwise answer 404 and hide which path is unreadable.
      logInternal("Filesystem access refused", error, "warn");
      return httpError(
         500,
         `The server cannot access a path it needs (${describeFilesystemAccessFailure(access)}). ` +
            `Give the user the server runs as access to it.`,
      );
   }
   const row = errorMapping(error);
   const logged = resolve(row.log, error);
   if (logged) logInternal(logged.summary, error, logged.level);
   const status = resolve(row.status, error);
   const message =
      row.message === undefined ? error.message : resolve(row.message, error);
   const headers = row.headers?.(error);
   if (row.problems) {
      return {
         status,
         ...(headers ? { headers } : {}),
         json: { code: status, message, problems: row.problems(error) },
      };
   }
   const answer = httpError(status, message, resolve(row.reason, error));
   return headers ? { ...answer, headers } : answer;
}

function resolve<E, T>(value: T | ((error: E) => T), error: E): T {
   return typeof value === "function"
      ? (value as (error: E) => T)(error)
      : value;
}

function httpError(
   code: number,
   message: string,
   reason?: ErrorReason,
): HttpErrorAnswer {
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

/** A loaded package whose build plan could not be derived at load. */
export class BuildPlanUnavailableError extends Error {
   get kind(): "build_plan_unavailable" {
      return "build_plan_unavailable";
   }

   constructor(message: string) {
      super(message);
      this.name = "BuildPlanUnavailableError";
   }
}

/**
 * A package's build plan is being re-derived after a connection change and
 * the wait for it passed its cap: answered 503 with Retry-After, rather than
 * holding the request until the re-derivation finishes.
 */
export class BuildPlanRefreshingError extends Error {
   get kind(): "build_plan_refreshing" {
      return "build_plan_refreshing";
   }

   /** Seconds a caller should wait before retrying, sent as Retry-After. */
   readonly retryAfterSeconds = 5;

   constructor(message: string) {
      super(message);
      this.name = "BuildPlanRefreshingError";
   }
}

export class NotImplementedError extends Error {
   get kind(): "not_implemented" {
      return "not_implemented";
   }

   constructor(message: string) {
      super(message);
   }
}

export class BadRequestError extends Error {
   get kind(): BadRequestKind {
      return "bad_request";
   }

   constructor(message: string) {
      super(message);
   }
}

/**
 * A specific argument was malformed; the message says which and what shape was
 * expected. A subclass so MCP advice does not offer Malloy syntax guidance for
 * a non-Malloy argument error. Still a 400.
 */
export class InvalidArgumentError extends BadRequestError {
   override get kind(): "invalid_argument" {
      return "invalid_argument";
   }
}

/**
 * A dashboard write was refused because the text does not compile; the message
 * names each problem with its line and column. A subclass so telemetry counts
 * a Malloy mistake apart from an API misuse without parsing message text.
 * Still a 400.
 */
export class CompileRefusedError extends BadRequestError {
   override get kind(): CompileRefusedKind {
      return "compile_refused";
   }
}

/**
 * A model path the write path refuses: unsafe as written, the root
 * `index.malloy`, in `public/`, or leading (through a link) outside the
 * package. A subclass so the write metric counts it as `invalid_path` wherever
 * it is raised; still a 400.
 */
export class InvalidModelPathError extends BadRequestError {
   override get kind(): "invalid_model_path" {
      return "invalid_model_path";
   }
}

/**
 * A query request that names neither query text nor a query to run (or a view
 * without its source). A subclass so MCP advice can say how to name a view,
 * which it used to try by matching a message no throw site produced. Still a 400.
 */
export class InvalidQueryRequestError extends BadRequestError {
   override get kind(): "invalid_query_request" {
      return "invalid_query_request";
   }
}

/** A document's text carries a URL-producing render tag or markup in a label; counted apart from a restricted construct. */
export class RenderTagRefusedError extends CompileRefusedError {
   override get kind(): "render_tag_refused" {
      return "render_tag_refused";
   }
}

/** The restricted-construct gate could not parse the text, so it judged nothing: a refusal for a fragment, a plain compile problem for one tile of a document. */
export class UnparseableTextError extends CompileRefusedError {
   override get kind(): "unparseable_text" {
      return "unparseable_text";
   }

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
   get kind(): "environment_not_found" {
      return "environment_not_found";
   }

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
   get kind(): "package_not_found" {
      return "package_not_found";
   }

   constructor(message: string, options?: ErrorOptions) {
      super(message, options);
   }
}

export class ModelNotFoundError extends Error {
   get kind(): "model_not_found" {
      return "model_not_found";
   }

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
   get kind(): "dashboard_not_found" {
      return "dashboard_not_found";
   }

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
   get kind(): "skill_not_found" {
      return "skill_not_found";
   }

   constructor(message: string) {
      super(message);
   }
}

/** No agent with that name is served for the package, whether undeclared or dropped by validation. */
export class AgentNotFoundError extends Error {
   get kind(): "agent_not_found" {
      return "agent_not_found";
   }

   constructor(message: string) {
      super(message);
   }
}

/** A request that names more than one thing where it must name one. 409. */
export class ConflictError extends Error {
   get kind(): "conflict" {
      return "conflict";
   }

   constructor(message: string) {
      super(message);
      this.name = "ConflictError";
   }
}

export class ConnectionNotFoundError extends Error {
   get kind(): "connection_not_found" {
      return "connection_not_found";
   }

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
   get kind(): "table_not_found" {
      return "table_not_found";
   }

   constructor(message: string) {
      super(message);
   }
}

export class ConnectionError extends Error {
   get kind(): ConnectionKind {
      return "connection";
   }

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
export class ConnectionFailedError extends ConnectionError {
   override get kind(): "connection_failed" {
      return "connection_failed";
   }
}

/**
 * A model names a connection the environment does not have, usually one
 * deleted after the package loaded: 424 with `reason: CONNECTION_NOT_FOUND`.
 * Distinct from {@link ConnectionNotFoundError}, the 404 for a name the caller
 * typed into a connection route.
 */
export class UnconfiguredConnectionError extends Error {
   get kind(): "unconfigured_connection" {
      return "unconfigured_connection";
   }

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
   override get kind(): "connection_pool_exhausted" {
      return "connection_pool_exhausted";
   }

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
   get kind(): "destination_not_found" {
      return "destination_not_found";
   }

   constructor(message: string) {
      super(message);
   }
}

/** The storage destination a request's path addresses is not configured: 404. */
export class StorageDestinationNotFoundError extends Error {
   get kind(): "storage_destination_not_found" {
      return "storage_destination_not_found";
   }

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
   get kind(): "storage_catalog_not_found" {
      return "storage_catalog_not_found";
   }

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
   get kind(): "connection_auth" {
      return "connection_auth";
   }

   constructor(message: string) {
      super(message);
   }
}

// A reachable catalog whose on-disk format the pinned DuckLake extension cannot
// attach (see ducklake_version.ts). 422, so it does not read as a credentials
// problem.
export class UnsupportedCatalogFormatError extends Error {
   get kind(): "unsupported_catalog_format" {
      return "unsupported_catalog_format";
   }

   constructor(message: string) {
      super(message);
   }
}

export class ModelCompilationError extends Error {
   get kind(): "model_compilation" {
      return "model_compilation";
   }

   // Any message-bearing object, so callers can add context and keep the 424.
   constructor(error: { message: string }) {
      super(error.message);
   }
}

/**
 * A published version's files are missing and could not be fetched again
 * from where it was published (unreachable, or changed since). 424: the
 * dependency the version needs is what failed, not the request.
 */
export class VersionFilesMissingError extends Error {
   get kind(): "version_files_missing" {
      return "version_files_missing";
   }

   constructor(message: string) {
      super(message);
      this.name = "VersionFilesMissingError";
   }
}

/**
 * The package's publisher.json cannot be used as written, so the package is not
 * served. 424, like a model that does not compile. The worker flags it
 * `isManifestError` so the pool restores the class instead of reporting the
 * author's typo as a 503 outage.
 */
export class PackageManifestError extends Error {
   get kind(): "package_manifest" {
      return "package_manifest";
   }

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
   get kind(): "materialization_eligibility" {
      return "materialization_eligibility";
   }

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
   get kind(): "chained_upstream_missing" {
      return "chained_upstream_missing";
   }

   readonly missing: readonly string[];
   /**
    * Why each missing upstream could not be used, by name, when the thrower
    * knows: lets a caller act on one cause (an entry at the wrong tier)
    * without parsing the joined message.
    */
   readonly details?: readonly { name: string; detail: string }[];

   constructor(
      missing: readonly string[],
      detail: string,
      details?: readonly { name: string; detail: string }[],
   ) {
      super(detail);
      this.name = "ChainedUpstreamMissingError";
      this.missing = missing;
      if (details) this.details = details;
   }
}

/**
 * A chained `storage=` build had every stored upstream, but the model assembled
 * over them did not compile: a limit of what the build can carry, not of the
 * source. Recomputing from raw would rebuild the handed-in tables, so under
 * `strictUpstreams` it is refused like a missing upstream.
 */
export class ChainedShapeNotCarriedError extends Error {
   get kind(): "chained_shape_not_carried" {
      return "chained_shape_not_carried";
   }

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
   get kind(): "publisher_config" {
      return "publisher_config";
   }

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
   get kind(): "server_configuration" {
      return "server_configuration";
   }

   constructor(message: string) {
      super(message);
      this.name = "ServerConfigurationError";
   }
}

export class FrozenConfigError extends Error {
   get kind(): "frozen_config" {
      return "frozen_config";
   }

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
   get kind(): "access_denied" {
      return "access_denied";
   }

   /**
    * The source a gate refused, when the refusal names one: what MCP advice
    * reads to say which source needs givens, instead of parsing the message.
    */
   readonly source?: string;

   constructor(message: string, options?: { source?: string }) {
      super(message);
      this.name = "AccessDeniedError";
      if (options?.source !== undefined) this.source = options.source;
   }

   /** The gate refusal for `source`. The message names the source and never the gate's logic. */
   static forSource(source: string): AccessDeniedError {
      return new AccessDeniedError(`Access denied for source "${source}".`, {
         source,
      });
   }
}

/**
 * Caller-submitted query text that did not compile, with problem ranges in the
 * text as the caller sent it. Extends MalloyError so class-based compile
 * classification still applies.
 */
export class QueryCompileError extends MalloyError {
   get kind(): "query_compile" {
      return "query_compile";
   }

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
   get kind(): NotQueryableKind {
      return "not_queryable";
   }

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
   override get kind(): "off_surface" {
      return "off_surface";
   }

   constructor(message: string) {
      super(message);
      this.name = "OffSurfaceError";
   }
}

export class MaterializationNotFoundError extends Error {
   get kind(): "materialization_not_found" {
      return "materialization_not_found";
   }

   constructor(message: string) {
      super(message);
   }
}

export class MaterializationConflictError extends Error {
   get kind(): "materialization_conflict" {
      return "materialization_conflict";
   }

   constructor(message: string) {
      super(message);
   }
}

/**
 * A write that found the file other than the caller expected: someone else
 * saved first (`expectedHash` no longer matches), or a create-only write
 * found a file already at the path (`reason` FILE_EXISTS, so a caller
 * choosing a name can tell it to try the next).
 */
export class WriteConflictError extends Error {
   get kind(): "write_conflict" {
      return "write_conflict";
   }

   constructor(
      message: string,
      readonly reason?: "FILE_EXISTS",
   ) {
      super(message);
      this.name = "WriteConflictError";
   }
}

/**
 * A write to a package installed from a `location`, which a reload would
 * discard: a reload fetches the package again from there and replaces its
 * files. 409, `reason` PACKAGE_INSTALLED_FROM_LOCATION. Interim, until package
 * storage keeps server-side writes apart from the fetched copy (#1560).
 */
export class InstalledFromLocationError extends Error {
   get kind(): "installed_from_location" {
      return "installed_from_location";
   }

   constructor(message: string) {
      super(message);
      this.name = "InstalledFromLocationError";
   }
}

/**
 * A write that was applied and then rolled back because the package would not
 * serve it: 500, the server's failure. The underlying failure is the `cause`
 * and is logged, never put in the message, since it can carry a path.
 */
export class WriteRolledBackError extends Error {
   get kind(): "write_rolled_back" {
      return "write_rolled_back";
   }

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
   get kind(): "write_rollback_failed" {
      return "write_rollback_failed";
   }

   constructor(message: string, options?: ErrorOptions) {
      super(message, options);
      this.name = "WriteRollbackFailedError";
   }
}

/**
 * An environment change that was applied, but whose connection rows could not
 * all be saved: those connections serve now and are wrong after a restart.
 * 500, the server's storage failing. The message is server-composed and names
 * only the connections; each underlying failure was logged where it happened.
 */
export class ConnectionSaveError extends Error {
   get kind(): "connection_save" {
      return "connection_save";
   }

   constructor(
      message: string,
      readonly connectionNames: readonly string[],
   ) {
      super(message);
      this.name = "ConnectionSaveError";
   }
}

/** A refusal from a write's post-reload check, worded for the caller. */
export class WriteVerifyError extends Error {
   get kind(): "write_verify" {
      return "write_verify";
   }

   constructor(message: string, options?: ErrorOptions) {
      super(message, options);
      this.name = "WriteVerifyError";
   }
}

export class InvalidStateTransitionError extends Error {
   get kind(): "invalid_state_transition" {
      return "invalid_state_transition";
   }

   constructor(message: string) {
      super(message);
   }
}

/** Temporary refusal (e.g. to stay under `PUBLISHER_MAX_MEMORY_BYTES`): 503, retry with back-off. */
export class ServiceUnavailableError extends Error {
   get kind(): ServiceUnavailableKind {
      return "service_unavailable";
   }

   constructor(message: string, options?: ErrorOptions) {
      super(message, options);
   }
}

/**
 * The memory governor refused to admit a new compiled copy of a package: a
 * 503 the caller answers by placing the package elsewhere, not a fault.
 */
export class PackageAdmissionRefusedError extends ServiceUnavailableError {
   override get kind(): PackageAdmissionKind {
      return "package_admission_refused";
   }
}

/**
 * A dry-run publish refused because the server is already running as many as
 * it allows (`MAX_CONCURRENT_DRY_RUNS`): a 503 like an admission
 * refusal, which a caller answers by retrying or asking another server.
 */
export class DryRunCapacityError extends PackageAdmissionRefusedError {
   override get kind(): "dry_run_capacity" {
      return "dry_run_capacity";
   }
}

/**
 * A response would exceed a server-side size cap (e.g.
 * `PUBLISHER_MAX_QUERY_ROWS`): 413, refine the query or raise the cap, not retry.
 */
export class PayloadTooLargeError extends Error {
   get kind(): PayloadTooLargeKind {
      return "payload_too_large";
   }

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
   override get kind(): "response_unserializable" {
      return "response_unserializable";
   }

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
   get kind(): "query_timeout" {
      return "query_timeout";
   }

   constructor(message: string) {
      super(message);
   }
}

// ---------------------------------------------------------------------------
// The error table: one row per error kind, read by both transports.
//
// internalErrorToHttpError (REST) and classifyToolError (MCP, in
// mcp/handler_utils.ts) both answer from ERROR_TABLE, keyed by errorKind(). A
// kind is the error's MOST DERIVED class, so a subclass answers from its own
// row whatever its parent's row says: there is no if-chain whose order decides
// which `instanceof` wins (InvalidArgumentError over BadRequestError,
// OffSurfaceError over NotQueryableError, ConnectionFailedError over
// ConnectionError, ResponseUnserializableError over PayloadTooLargeError). The
// one check that is not a kind, a refused filesystem access on the `cause`
// chain, is REST-only and runs before the table (see internalErrorToHttpError).
//
// Adding a class: give it a `kind` getter returning a new literal, add it to
// ErrorByKind and ERROR_CLASSES, and give it a row; the typecheck fails until
// all three exist. A class that does not override its parent's `kind` answers
// from its parent's row, and errors_mapping.snapshot.spec.ts fails until it
// gets its own.
// ---------------------------------------------------------------------------

type CompileRefusedKind =
   | "compile_refused"
   | "render_tag_refused"
   | "unparseable_text";
type BadRequestKind =
   | "bad_request"
   | "invalid_argument"
   | "invalid_model_path"
   | "invalid_query_request"
   | CompileRefusedKind;
type ConnectionKind =
   | "connection"
   | "connection_failed"
   | "connection_pool_exhausted";
type NotQueryableKind = "not_queryable" | "off_surface";
type PackageAdmissionKind = "package_admission_refused" | "dry_run_capacity";
type ServiceUnavailableKind = "service_unavailable" | PackageAdmissionKind;
type PayloadTooLargeKind = "payload_too_large" | "response_unserializable";

/** Each kind's error type: a class of this module, or a foreign error the mappers recognize. */
interface ErrorByKind {
   package_version: PackageVersionError;
   build_plan_unavailable: BuildPlanUnavailableError;
   build_plan_refreshing: BuildPlanRefreshingError;
   not_implemented: NotImplementedError;
   bad_request: BadRequestError;
   invalid_argument: InvalidArgumentError;
   compile_refused: CompileRefusedError;
   invalid_model_path: InvalidModelPathError;
   invalid_query_request: InvalidQueryRequestError;
   render_tag_refused: RenderTagRefusedError;
   unparseable_text: UnparseableTextError;
   environment_not_found: EnvironmentNotFoundError;
   package_not_found: PackageNotFoundError;
   model_not_found: ModelNotFoundError;
   dashboard_not_found: DashboardNotFoundError;
   skill_not_found: SkillNotFoundError;
   agent_not_found: AgentNotFoundError;
   conflict: ConflictError;
   connection_not_found: ConnectionNotFoundError;
   table_not_found: TableNotFoundError;
   connection: ConnectionError;
   connection_failed: ConnectionFailedError;
   unconfigured_connection: UnconfiguredConnectionError;
   connection_pool_exhausted: ConnectionPoolExhaustedError;
   destination_not_found: DestinationNotFoundError;
   storage_destination_not_found: StorageDestinationNotFoundError;
   storage_catalog_not_found: StorageCatalogNotFoundError;
   connection_auth: ConnectionAuthError;
   unsupported_catalog_format: UnsupportedCatalogFormatError;
   model_compilation: ModelCompilationError;
   version_files_missing: VersionFilesMissingError;
   package_manifest: PackageManifestError;
   materialization_eligibility: MaterializationEligibilityError;
   chained_upstream_missing: ChainedUpstreamMissingError;
   chained_shape_not_carried: ChainedShapeNotCarriedError;
   publisher_config: PublisherConfigError;
   server_configuration: ServerConfigurationError;
   frozen_config: FrozenConfigError;
   access_denied: AccessDeniedError;
   query_compile: QueryCompileError;
   not_queryable: NotQueryableError;
   off_surface: OffSurfaceError;
   materialization_not_found: MaterializationNotFoundError;
   materialization_conflict: MaterializationConflictError;
   write_conflict: WriteConflictError;
   installed_from_location: InstalledFromLocationError;
   write_rolled_back: WriteRolledBackError;
   write_rollback_failed: WriteRollbackFailedError;
   connection_save: ConnectionSaveError;
   write_verify: WriteVerifyError;
   invalid_state_transition: InvalidStateTransitionError;
   service_unavailable: ServiceUnavailableError;
   package_admission_refused: PackageAdmissionRefusedError;
   dry_run_capacity: DryRunCapacityError;
   payload_too_large: PayloadTooLargeError;
   response_unserializable: ResponseUnserializableError;
   query_timeout: QueryTimeoutError;
   // Foreign errors: no class of ours, so recognized by what they are.
   /** A MalloyError from @malloydata/malloy that is not our QueryCompileError. */
   malloy: MalloyError;
   /** A request body body-parser refused (an http-errors error with `expose`). */
   body_parser: Error;
   /** Anything else, including a thrown non-Error. */
   unknown: unknown;
}

/** A stable name for what an error is, shared by every transport that answers it. */
export type ErrorKind = keyof ErrorByKind;
type ForeignKind = "malloy" | "body_parser" | "unknown";
type ClassKind = Exclude<ErrorKind, ForeignKind>;

/**
 * The class each class kind belongs to. An error's `kind` is trusted only when
 * the error is an instance of this class, so a foreign error that happens to
 * carry a `kind` string is not mistaken for one of ours. The conditional type
 * fails the typecheck for a class whose `kind` cannot return its key.
 */
const ERROR_CLASSES: {
   [K in ClassKind]: K extends ErrorByKind[K]["kind"]
      ? abstract new (...args: never[]) => ErrorByKind[K]
      : never;
} = {
   package_version: PackageVersionError,
   build_plan_unavailable: BuildPlanUnavailableError,
   build_plan_refreshing: BuildPlanRefreshingError,
   not_implemented: NotImplementedError,
   bad_request: BadRequestError,
   invalid_argument: InvalidArgumentError,
   compile_refused: CompileRefusedError,
   invalid_model_path: InvalidModelPathError,
   invalid_query_request: InvalidQueryRequestError,
   render_tag_refused: RenderTagRefusedError,
   unparseable_text: UnparseableTextError,
   environment_not_found: EnvironmentNotFoundError,
   package_not_found: PackageNotFoundError,
   model_not_found: ModelNotFoundError,
   dashboard_not_found: DashboardNotFoundError,
   skill_not_found: SkillNotFoundError,
   agent_not_found: AgentNotFoundError,
   conflict: ConflictError,
   connection_not_found: ConnectionNotFoundError,
   table_not_found: TableNotFoundError,
   connection: ConnectionError,
   connection_failed: ConnectionFailedError,
   unconfigured_connection: UnconfiguredConnectionError,
   connection_pool_exhausted: ConnectionPoolExhaustedError,
   destination_not_found: DestinationNotFoundError,
   storage_destination_not_found: StorageDestinationNotFoundError,
   storage_catalog_not_found: StorageCatalogNotFoundError,
   connection_auth: ConnectionAuthError,
   unsupported_catalog_format: UnsupportedCatalogFormatError,
   model_compilation: ModelCompilationError,
   version_files_missing: VersionFilesMissingError,
   package_manifest: PackageManifestError,
   materialization_eligibility: MaterializationEligibilityError,
   chained_upstream_missing: ChainedUpstreamMissingError,
   chained_shape_not_carried: ChainedShapeNotCarriedError,
   publisher_config: PublisherConfigError,
   server_configuration: ServerConfigurationError,
   frozen_config: FrozenConfigError,
   access_denied: AccessDeniedError,
   query_compile: QueryCompileError,
   not_queryable: NotQueryableError,
   off_surface: OffSurfaceError,
   materialization_not_found: MaterializationNotFoundError,
   materialization_conflict: MaterializationConflictError,
   write_conflict: WriteConflictError,
   installed_from_location: InstalledFromLocationError,
   write_rolled_back: WriteRolledBackError,
   write_rollback_failed: WriteRollbackFailedError,
   connection_save: ConnectionSaveError,
   write_verify: WriteVerifyError,
   invalid_state_transition: InvalidStateTransitionError,
   service_unavailable: ServiceUnavailableError,
   package_admission_refused: PackageAdmissionRefusedError,
   dry_run_capacity: DryRunCapacityError,
   payload_too_large: PayloadTooLargeError,
   response_unserializable: ResponseUnserializableError,
   query_timeout: QueryTimeoutError,
};

/** Every class kind's class, for a spec that checks each declares its own `kind`. */
export const ERROR_CLASSES_FOR_TESTS: Readonly<
   Record<ClassKind, abstract new (...args: never[]) => Error>
> = ERROR_CLASSES;

/** What `error` is, for the error table. */
export function errorKind(error: unknown): ErrorKind {
   if (error instanceof Error) {
      const kind = (error as { kind?: unknown }).kind;
      if (
         typeof kind === "string" &&
         Object.hasOwn(ERROR_CLASSES, kind) &&
         error instanceof ERROR_CLASSES[kind as ClassKind]
      ) {
         return kind as ClassKind;
      }
      if (error instanceof MalloyError) return "malloy";
      if (bodyParserClientStatus(error) !== undefined) return "body_parser";
   }
   return "unknown";
}

/** A line {@link internalErrorToHttpError} logs, where the body stops carrying the detail. */
interface LogLine {
   summary: string;
   level: "warn" | "error";
}

/** Where an MCP tool failed, for advice that names it. */
export interface McpSite {
   /** The operation that failed, e.g. `executeQuery`. */
   operation: string;
   /** The resource it failed on, e.g. `env/package`. */
   identifier: string;
}

/**
 * The agent-facing answer to an error over MCP.
 *
 * - `fixed`: this message (the error's own when absent) and these suggestions;
 *   `log` names the summary logged, as `<log> during <operation>`, when the
 *   error's text is withheld from the agent.
 * - `not_found`: the generic "Resource not found" for the site's identifier.
 * - `environment_not_found`: the environments that do exist, when the error
 *   carries them; otherwise `not_found`.
 * - `malloy`: the Malloy advice, refined by what the error says it is
 *   (mcp/error_messages.ts getMalloyErrorDetails).
 * - `internal`: reported as an unexpected internal failure.
 */
export type McpAdvice<E> =
   | {
        advice: "fixed";
        message?: (error: E, site: McpSite) => string;
        suggestions: readonly string[];
        log?: string;
     }
   | { advice: "not_found" }
   | { advice: "environment_not_found" }
   | { advice: "malloy" }
   | { advice: "internal" };

/**
 * Which part of an MCP setup request (environment, package, model) a setup
 * failure names, for mapQuerySetupError. `resource` is the whole
 * `env/package/model`. Absent: the failure is reported as an internal setup
 * fault rather than classified.
 */
export type McpSetupScope = "environment" | "package" | "model" | "resource";

/** One row of the error table. */
export interface ErrorMapping<E> {
   /** The HTTP status, or how the error decides it. */
   status: number | ((error: E) => number);
   /** The HTTP body's message; the error's own when absent. */
   message?: string | ((error: E) => string);
   /** The HTTP body's `reason`, for a caller that branches on it. */
   reason?: ErrorReason | ((error: E) => ErrorReason | undefined);
   /** Logged server-side by the HTTP mapping (unless the caller passed `log: false`). */
   log?: LogLine | ((error: E) => LogLine | undefined);
   /** The HTTP body also carries the compile problems. */
   problems?: (error: E) => HttpErrorAnswer["json"]["problems"];
   /** Response headers to send with the error (Retry-After on a 503). */
   headers?: (error: E) => Record<string, string>;
   mcp: McpAdvice<E>;
   mcpSetup?: McpSetupScope;
}

const UNHANDLED_LOG: LogLine = {
   summary: "Unhandled internal error",
   level: "error",
};

/** A failure this module has no specific answer for: a generalized 500, reported as internal over MCP. */
const UNHANDLED = {
   status: 500,
   message: GENERIC_INTERNAL_MESSAGE,
   log: UNHANDLED_LOG,
   mcp: { advice: "internal" },
} as const satisfies ErrorMapping<unknown>;

const NOT_TRANSIENT_CHANGE_QUERY =
   "This is not transient. The same query will fail the same way, so change the query rather than retrying it.";

/**
 * Remediation for a ServiceUnavailableError. It covers both the memory governor
 * and the concurrency slots, so the wording names both.
 */
export const BACK_PRESSURE_SUGGESTIONS: readonly string[] = [
   "Retry once the publisher is below its configured memory or concurrency limit.",
   "If this persists, raise the limit or scale up the pod.",
];

const BACK_PRESSURE = {
   status: 503,
   mcp: { advice: "fixed", suggestions: BACK_PRESSURE_SUGGESTIONS },
   mcpSetup: "resource",
} as const satisfies ErrorMapping<ServiceUnavailableError>;

const TOO_LARGE = {
   status: 413,
   mcp: {
      advice: "fixed",
      // The message already says to refine the query; this adds that it is
      // not transient, which is why it is a 413 and not the retryable 503.
      suggestions: [
         NOT_TRANSIENT_CHANGE_QUERY,
         "If it is already minimal, raise the configured cap named in the message.",
      ],
   },
} as const satisfies ErrorMapping<PayloadTooLargeError>;

/** A request the caller must change: 400, with Malloy advice over MCP. */
const BAD_REQUEST = {
   status: 400,
   mcp: { advice: "malloy" },
} as const satisfies ErrorMapping<BadRequestError>;

const NOT_FOUND = {
   status: 404,
   mcp: { advice: "not_found" },
} as const satisfies ErrorMapping<Error>;

/** A server-authored message is returned; a driver's is logged and generalized. */
const CONNECTION = {
   status: 502,
   // It can name an internal host, echo SQL, or act as a failure-mode oracle.
   // This also hides a statement the warehouse rejected (for every dialect but
   // Postgres, see statementRejection), because ConnectionError does not
   // distinguish a rejected statement from a transport failure.
   message: (error: ConnectionError) =>
      error.callerSafe ? error.message : GENERIC_UPSTREAM_MESSAGE,
   log: (error: ConnectionError): LogLine | undefined =>
      error.callerSafe
         ? undefined
         : { summary: "Upstream connection error", level: "warn" },
   mcp: { advice: "internal" },
} as const satisfies ErrorMapping<ConnectionError>;

/**
 * The table. Typed as a mapped type over every kind, so a kind with no row,
 * or a row for a kind that does not exist, fails the typecheck.
 */
export const ERROR_TABLE: { [K in ErrorKind]: ErrorMapping<ErrorByKind[K]> } = {
   package_version: {
      status: (error) => error.status,
      reason: (error) => error.reason,
      mcp: {
         // An unknown, archived or malformed version, or no `latest` yet.
         // Not transient and not the model's fault; the message says which.
         advice: "fixed",
         suggestions: [
            "This is not transient: retrying fails the same way until the package's versions change.",
            "The MCP tools serve the package's latest version. A package whose versions have no latest yet must have one made latest (PUT .../packages/{package}/latest) before these tools can read it.",
         ],
      },
      // A package with versions and no `latest` yet: its own message.
      mcpSetup: "package",
   },
   bad_request: BAD_REQUEST,
   // A malformed argument that is not about Malloy, so it must not get the
   // Malloy advice. BadRequestError keeps it: that class also wraps genuine
   // query-time compile failures, which do want the Malloy advice.
   invalid_argument: {
      status: 400,
      mcp: {
         advice: "fixed",
         suggestions: [
            "This is not transient. The same arguments will fail the same way, so change them rather than retrying.",
            "The message above names what was wrong. If it names an expected format, use that format exactly.",
         ],
      },
   },
   compile_refused: BAD_REQUEST,
   invalid_model_path: BAD_REQUEST,
   invalid_query_request: BAD_REQUEST,
   render_tag_refused: BAD_REQUEST,
   unparseable_text: BAD_REQUEST,
   server_configuration: {
      status: 500,
      log: { summary: "Server configuration error", level: "warn" },
      mcp: { advice: "internal" },
   },
   frozen_config: { status: 403, mcp: { advice: "internal" } },
   access_denied: {
      status: 403,
      mcp: { advice: "malloy" },
      // The #(authorize) gate fires at query time, not setup, but a denial
      // must never surface as an opaque internal error.
      mcpSetup: "resource",
   },
   environment_not_found: {
      status: 404,
      mcp: { advice: "environment_not_found" },
      mcpSetup: "environment",
   },
   package_not_found: { ...NOT_FOUND, mcpSetup: "package" },
   model_not_found: { ...NOT_FOUND, mcpSetup: "model" },
   dashboard_not_found: { status: 404, mcp: { advice: "internal" } },
   skill_not_found: { status: 404, mcp: { advice: "internal" } },
   agent_not_found: { status: 404, mcp: { advice: "internal" } },
   conflict: { status: 409, mcp: { advice: "internal" } },
   // A query-boundary denial is a deliberate 404; `not_found` also drops the
   // source name this class is careful not to confirm.
   not_queryable: NOT_FOUND,
   off_surface: {
      status: 404,
      mcp: {
         // Not `not_found`, which would drop this message and tell the agent
         // to check its spelling.
         advice: "fixed",
         suggestions: [
            "This is curation, not a typo: the name is real and the package does not publish it. Retrying with a different spelling will not help.",
            "To query what IS published, call get_context for this package and use the model_path it returns, verbatim.",
         ],
      },
      mcpSetup: "model",
   },
   query_compile: {
      status: 400,
      problems: (error) => error.problems.map(toQueryTextProblem),
      mcp: { advice: "malloy" },
   },
   malloy: { status: 400, mcp: { advice: "malloy" } },
   table_not_found: {
      status: 404,
      reason: "TABLE_NOT_FOUND",
      mcp: { advice: "internal" },
   },
   build_plan_unavailable: {
      status: 409,
      reason: "BUILD_PLAN_UNAVAILABLE",
      mcp: { advice: "internal" },
   },
   build_plan_refreshing: {
      status: 503,
      reason: "BUILD_PLAN_REFRESHING",
      headers: (error) => ({ "Retry-After": String(error.retryAfterSeconds) }),
      // Transient: a connection edit's re-derivation is running. Retryable
      // advice, not "internal", so an agent waits and retries.
      mcp: {
         advice: "fixed",
         suggestions: [
            "Retry in a few seconds: the build plan is being re-derived after a connection change.",
         ],
      },
      mcpSetup: "package",
   },
   // Otherwise reported as internal, and an agent retries it forever.
   connection_not_found: NOT_FOUND,
   storage_destination_not_found: {
      status: 404,
      mcp: { advice: "internal" },
   },
   storage_catalog_not_found: { status: 404, mcp: { advice: "internal" } },
   destination_not_found: { status: 422, mcp: { advice: "internal" } },
   connection_auth: {
      // The driver's text can name the user, the account or the host, so it
      // is logged and the answer says only what to fix. warn: a
      // misconfigured connection is the customer's to fix, not our fault.
      status: 424,
      message: GENERIC_AUTH_MESSAGE,
      reason: "CONNECTION_AUTH_FAILED",
      log: { summary: "Connection credentials rejected", level: "warn" },
      mcp: {
         advice: "fixed",
         message: (_error, { identifier }) =>
            `The database rejected the connection's credentials for ${identifier}. The query never ran.`,
         suggestions: [
            "The query and the model are fine. Do not rewrite them, and do not retry: it fails the same way until the credentials are fixed.",
            "Report that the connection's user, password, key or token needs updating.",
         ],
         log: "Connection credentials rejected",
      },
   },
   connection_failed: {
      // ConnectionError's 502 and logging, plus the reason that says the
      // database is down. The driver's text can name an internal host.
      status: 502,
      message: GENERIC_UNREACHABLE_MESSAGE,
      reason: "CONNECTION_FAILED",
      log: { summary: "Database unreachable", level: "warn" },
      mcp: {
         advice: "fixed",
         message: (_error, { identifier }) =>
            `The database connection for ${identifier} is down: the database could not be reached, so the query never ran.`,
         suggestions: [
            "The query and the model are fine. Do not rewrite them.",
            "Retry once. If it fails again, report that the database connection is down rather than changing the query.",
         ],
         log: "Database unreachable",
      },
   },
   unconfigured_connection: {
      status: 424,
      reason: "CONNECTION_NOT_FOUND",
      mcp: {
         advice: "fixed",
         message: (error, { identifier }) =>
            `${error.message}. The model for ${identifier} uses a connection this environment does not have, so the query never ran.`,
         suggestions: [
            "The query is fine. Do not rewrite it, and do not retry: it fails the same way until the connection exists.",
            "Report that the environment is missing this connection; it was likely deleted or renamed.",
         ],
      },
   },
   unsupported_catalog_format: { status: 422, mcp: { advice: "internal" } },
   materialization_eligibility: {
      status: 422,
      mcp: { advice: "internal" },
   },
   model_compilation: {
      status: 424,
      mcp: { advice: "malloy" },
      mcpSetup: "resource",
   },
   version_files_missing: { status: 424, mcp: { advice: "internal" } },
   package_manifest: {
      status: 424,
      mcp: {
         // An unusable publisher.json: neither "retry" (internal) nor
         // "check your .malloy files" (Malloy) applies.
         advice: "fixed",
         suggestions: [
            "This is not transient. The package's publisher.json is invalid, so retrying fails the same way until the file is fixed.",
            "Fix the field the message names in publisher.json, then call reload_package.",
         ],
      },
      mcpSetup: "package",
   },
   connection: CONNECTION,
   connection_pool_exhausted: {
      ...CONNECTION,
      mcp: {
         advice: "fixed",
         suggestions: [
            "Retry shortly; this clears as other queries on the connection finish.",
            "If it persists, run fewer queries at once on this connection, or ask the operator to raise PUBLISHER_POSTGRES_POOL_MAX.",
         ],
      },
   },
   materialization_not_found: { status: 404, mcp: { advice: "internal" } },
   materialization_conflict: { status: 409, mcp: { advice: "internal" } },
   invalid_state_transition: { status: 409, mcp: { advice: "internal" } },
   write_conflict: {
      status: 409,
      reason: (error) => error.reason,
      mcp: { advice: "internal" },
   },
   installed_from_location: {
      status: 409,
      reason: "PACKAGE_INSTALLED_FROM_LOCATION",
      mcp: {
         // Not transient, and not the model's fault: the message names where
         // the change belongs.
         advice: "fixed",
         suggestions: [
            "This is not transient: retrying fails the same way while the package is installed from a location.",
            "Make the change in the package's source at that location and reload the package, or publish it as a version.",
         ],
      },
   },
   write_rollback_failed: { status: 500, mcp: { advice: "internal" } },
   // Answered with its message, which names only connections; each failure
   // was logged where it happened, so it is not logged again here.
   connection_save: { status: 500, mcp: { advice: "internal" } },
   // Logged once, where the rollback happened, with the cause.
   write_rolled_back: { status: 500, mcp: { advice: "internal" } },
   service_unavailable: BACK_PRESSURE,
   package_admission_refused: BACK_PRESSURE,
   dry_run_capacity: BACK_PRESSURE,
   payload_too_large: TOO_LARGE,
   response_unserializable: {
      ...TOO_LARGE,
      mcp: {
         // Not TOO_LARGE's "raise the cap", which cannot help a response
         // that will not serialize at any cap.
         advice: "fixed",
         suggestions: [
            NOT_TRANSIENT_CHANGE_QUERY,
            "Raising the byte cap will not help, because the response cannot be serialized at any cap. Shrink it instead: project fewer columns, add a LIMIT, or filter out the wide values.",
         ],
      },
   },
   query_timeout: {
      status: 504,
      mcp: TOO_LARGE.mcp,
   },
   // A feature the server does not have (e.g. `versionId`) is not an
   // internal failure; the OpenAPI spec documents 501 on those routes.
   not_implemented: { status: 501, mcp: { advice: "internal" } },
   body_parser: {
      status: (error) => bodyParserClientStatus(error)!,
      mcp: { advice: "internal" },
   },
   // No specific answer: a generalized 500, its detail only logged.
   chained_upstream_missing: UNHANDLED,
   chained_shape_not_carried: UNHANDLED,
   publisher_config: UNHANDLED,
   write_verify: UNHANDLED,
   unknown: UNHANDLED,
};

/** The table row `error` answers from. */
export function errorMapping(error: unknown): ErrorMapping<Error> {
   // The row is typed for its kind; errorKind guarantees `error` is of it.
   return ERROR_TABLE[errorKind(error)] as ErrorMapping<Error>;
}
