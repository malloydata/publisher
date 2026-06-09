import { MalloyError } from "@malloydata/malloy";
import { PUBLISHER_CONFIG_NAME } from "./constants";

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
   } else if (error instanceof NotQueryableError) {
      return httpError(404, error.message);
   } else if (error instanceof MalloyError) {
      return httpError(400, error.message);
   } else if (error instanceof ConnectionNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof ConnectionAuthError) {
      return httpError(422, error.message);
   } else if (error instanceof ModelCompilationError) {
      return httpError(424, error.message);
   } else if (error instanceof ConnectionError) {
      return httpError(502, error.message);
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
   } else {
      return httpError(500, error.message);
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

export class ConnectionNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ConnectionError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ConnectionAuthError extends Error {
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
