/**
 * Classification + diagnostics for connection schema-introspection failures.
 *
 * Why this exists
 * ---------------
 * When a connection can't introspect a table/SQL schema for an
 * **infrastructure** reason — an expired token (HTTP 401), a forbidden
 * scope (403), or the data plane being down/unreachable/timing out
 * (5xx / network / timeout) — the failure travels a long, lossy path
 * before a developer ever sees it:
 *
 *   1. `PublisherConnection.extractAndThrowError` (in @malloydata/db-publisher)
 *      flattens the AxiosError to a bare `Error(error.response?.data?.message
 *      || error.message)`. For a 401 with no body that's just
 *      `"Request failed with status code 401"` — no status field, no
 *      connection name, no table path, no auth-vs-not-found signal.
 *   2. Malloy's `BaseConnection.fetchSchemaForTables` **catches** that throw
 *      per-table and returns it as a **string** in `result.errors[table]`.
 *   3. The Malloy compiler then can't resolve `conn.table('...')`, so the
 *      source and *every field that references it* emit their own
 *      "X is not defined" error — a cascade that reads like a broken
 *      Malloy model when the real cause is the connection.
 *
 * We cannot fix step 1 here (db-publisher is an installed dependency, not
 * in this repo's workspace), but the flattened message still *contains*
 * the HTTP status text, so we can classify it server-side and short-circuit
 * the cascade with one clear connection diagnostic. See
 * {@link PackageLoadPool.handleSchemaForTables}.
 *
 * Only **auth** and **transport** failures are treated as infrastructure
 * failures and short-circuited. A genuine **not_found** (404 / "table does
 * not exist") and anything we can't confidently classify ("other") are left
 * alone so they keep producing normal Malloy compile diagnostics — a real
 * modeling mistake must still surface as a modeling error.
 */
import { ConnectionAuthError, ConnectionError } from "../errors";

export type SchemaFetchFailureKind =
   | "auth"
   | "transport"
   | "not_found"
   | "other";

export interface SchemaFetchFailure {
   kind: SchemaFetchFailureKind;
   /** HTTP status, when one could be parsed from the error text. */
   status?: number;
   /** Connection name the model referenced (e.g. `prod_warehouse`). */
   connection: string;
   /** What we were introspecting — a table path or an inline SQL source. */
   target: string;
   /** The raw (flattened) error string from the connection layer. */
   rawMessage: string;
}

const HTTP_STATUS_TEXT: Record<number, string> = {
   400: "Bad Request",
   401: "Unauthorized",
   403: "Forbidden",
   404: "Not Found",
   408: "Request Timeout",
   429: "Too Many Requests",
   500: "Internal Server Error",
   502: "Bad Gateway",
   503: "Service Unavailable",
   504: "Gateway Timeout",
};

/**
 * Classify a schema-fetch failure from the (already-flattened) error string
 * the connection layer produced.
 *
 * Status-first, because the HTTP status is the most reliable signal and the
 * common case (`"Request failed with status code 401"`) carries it. Falls
 * back to keyword heuristics for failures that carry no HTTP status (raw
 * socket/DNS/timeout errors) or whose body text replaced the status line.
 */
export function classifySchemaFetchError(message: string): {
   kind: SchemaFetchFailureKind;
   status?: number;
} {
   const match =
      /status code (\d{3})/i.exec(message) ??
      /\bHTTP\s+(\d{3})\b/i.exec(message);
   const status = match ? Number(match[1]) : undefined;

   if (status === 401 || status === 403) return { kind: "auth", status };
   if (status === 404) return { kind: "not_found", status };
   if (status === 408 || (status !== undefined && status >= 500)) {
      return { kind: "transport", status };
   }

   // No decisive HTTP status — fall back to keyword heuristics.
   if (
      /\b(unauthori[sz]ed|forbidden|access denied|authentication failed|not authenticated|(token|credentials?)[^.]*\b(expired|invalid|missing)|(expired|invalid|missing)[^.]*\b(token|credentials?))\b/i.test(
         message,
      )
   ) {
      return { kind: "auth", status };
   }
   if (
      /(ECONNREFUSED|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|EPIPE|EHOSTUNREACH|ENETUNREACH|socket hang up|network error|timeout of \d+\s*ms exceeded|getaddrinfo)/i.test(
         message,
      )
   ) {
      return { kind: "transport", status };
   }
   if (
      /(not found|does ?n'?o?t exist|no such table|unknown table)/i.test(
         message,
      )
   ) {
      return { kind: "not_found", status };
   }
   return { kind: "other", status };
}

/**
 * True for failures caused by the connection/credentials/transport rather
 * than the Malloy model. Only these are short-circuited; everything else
 * keeps producing normal compile diagnostics.
 */
export function isInfrastructureFailure(kind: SchemaFetchFailureKind): boolean {
   return kind === "auth" || kind === "transport";
}

/**
 * Build a single, developer-facing diagnostic that names the connection, the
 * table/SQL being introspected, the HTTP status, and the likely cause —
 * explicitly telling the reader this is *not* a Malloy model error, so they
 * don't go chasing the "X is not defined" cascade.
 */
export function buildConnectionDiagnostic(failure: SchemaFetchFailure): string {
   const { kind, status, connection, target, rawMessage } = failure;
   const statusText =
      status !== undefined ? HTTP_STATUS_TEXT[status] : undefined;
   const statusPhrase =
      status !== undefined
         ? `returned ${status}${statusText ? ` ${statusText}` : ""}`
         : "could not be reached";

   const cause =
      kind === "auth"
         ? "The access token is likely expired or invalid; refresh the connection and reload."
         : "The data plane may be down, unreachable, or timing out; check the connection and retry.";

   return (
      `Connection "${connection}" ${statusPhrase} while introspecting "${target}". ` +
      `${cause} ` +
      `This is a connection problem, not an error in your Malloy model ` +
      `(underlying error: ${rawMessage}).`
   );
}

/**
 * The typed Error to fail the package load with, so the HTTP layer maps it
 * correctly: `ConnectionAuthError` → 422, `ConnectionError` → 502 (see
 * `internalErrorToHttpError`). Only called for infrastructure failures.
 */
export function connectionFailureToError(failure: SchemaFetchFailure): Error {
   const message = buildConnectionDiagnostic(failure);
   return failure.kind === "auth"
      ? new ConnectionAuthError(message)
      : new ConnectionError(message);
}
