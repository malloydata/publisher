// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyError } from "@malloydata/malloy";
import { PUBLISHER_CONFIG_NAME } from "./constants";
import { logger } from "./logger";
import type { EligibilityRefusalReason } from "./materialization_metrics";

// Client-facing body for an internal failure (500/502). The specific error
// message can carry internal detail -- a filesystem path, an SQL fragment, an
// upstream host -- so it is logged server-side (below) and NOT returned. Every
// other status this mapper produces is a client error (4xx) whose message is
// actionable to the caller and is returned as-is.
const GENERIC_INTERNAL_MESSAGE = "Internal server error.";
const GENERIC_UPSTREAM_MESSAGE = "Upstream connection error.";

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
 * The fields are copied out explicitly rather than passed as `{ error }`:
 * `message` and `stack` are non-enumerable own properties of `Error`, so
 * `logger.error(msg, { error })` serializes to `{"error":{}}` under both formats
 * this server configures -- the detail would exist nowhere at all. A bare splat
 * (`logger.error(msg, error)`) does carry both, but copying the fields is what
 * lets the two guards below apply to them.
 *
 * Newlines and other control characters are stripped because the default format
 * (colorize + simple, whenever OTEL_EXPORTER_OTLP_ENDPOINT is unset) is
 * newline-delimited plain text, so a message carrying `\n` -- and caller SQL can
 * -- could otherwise forge log entries. The range also covers the separators
 * JSON.stringify does NOT escape (NEL, and the U+2028/U+2029 line and paragraph
 * separators): those reach the rendered line verbatim under both formats, so
 * `format.json()` is not a backstop for them the way it is for `\n`.
 */
export function logInternalFailure(summary: string, error: Error): void {
   const sanitize = (value: string): string =>
      // eslint-disable-next-line no-control-regex
      value
         .replace(/[\u0000-\u001f\u007f-\u009f\u2028\u2029]/g, " ")
         .slice(0, MAX_LOGGED_DETAIL_CHARS);
   logger.error(summary, {
      name: error.name,
      message: sanitize(error.message ?? ""),
      stack: sanitize(error.stack ?? ""),
   });
}

export function internalErrorToHttpError(error: Error) {
   if (error instanceof BadRequestError) {
      return httpError(400, error.message);
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
   } else if (error instanceof NotQueryableError) {
      return httpError(404, error.message);
   } else if (error instanceof MalloyError) {
      return httpError(400, error.message);
   } else if (error instanceof ConnectionNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof DestinationNotFoundError) {
      return httpError(422, error.message);
   } else if (error instanceof ConnectionAuthError) {
      return httpError(422, error.message);
   } else if (error instanceof UnsupportedCatalogFormatError) {
      return httpError(422, error.message);
   } else if (error instanceof MaterializationEligibilityError) {
      return httpError(422, error.message);
   } else if (error instanceof ModelCompilationError) {
      return httpError(424, error.message);
   } else if (error instanceof ConnectionError) {
      // 502. A server-authored message (see ConnectionError.callerSafe) is
      // actionable and returned as-is; anything wrapping a driver message is
      // logged and generalized, because it can name an internal host/port, echo
      // the caller's SQL, or distinguish refused from timed-out from auth-failed.
      if (error.callerSafe) {
         return httpError(502, error.message);
      }
      logInternalFailure("Upstream connection error", error);
      return httpError(502, GENERIC_UPSTREAM_MESSAGE);
   } else if (error instanceof MaterializationNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof MaterializationConflictError) {
      return httpError(409, error.message);
   } else if (error instanceof InvalidStateTransitionError) {
      return httpError(409, error.message);
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
      logInternalFailure("Unhandled internal error", error);
      return httpError(500, GENERIC_INTERNAL_MESSAGE);
   }
}

function httpError(code: number, message: string) {
   return {
      status: code,
      json: {
         code,
         message: message,
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

export class EnvironmentNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class PackageNotFoundError extends Error {
   constructor(message: string) {
      super(message);
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

export class FrozenConfigError extends Error {
   constructor(
      message = `Publisher config can't be updated when ${PUBLISHER_CONFIG_NAME} has { "frozenConfig": true }`,
   ) {
      super(message);
   }
}

/**
 * A request was denied by a source's `#(authorize)` gate (HTTP 403). Thrown by
 * the runtime authorize check when no in-scope expression evaluates true for
 * the supplied givens (including when a referenced given has no value).
 */
export class AccessDeniedError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "AccessDeniedError";
   }
}

/**
 * A query targeted a source/model that is not part of the package's queryable
 * surface under `queryableSources: "declared"` (a non-`explores` model file, or
 * a source not in a model's `export {}` closure). Mapped to HTTP **404**, not
 * 403, and with a deliberately generic message: unlike `#(authorize)` (which is
 * identity-scoped and answers "who"), the explore boundary is identity-free and
 * answers "what is queryable" — so a hidden target should be indistinguishable
 * from a non-existent one (no enumeration / existence oracle).
 */
export class NotQueryableError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "NotQueryableError";
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
   constructor(message: string) {
      super(message);
   }
}

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
