// Postgres / libpq helpers shared between `service/` (user-facing
// connections) and `storage/` (materialization-storage catalog). Lives at
// `src/` root so neither layer takes a dependency on the other — see
// CLAUDE.md's "Two parallel DuckLake/PG attach paths" note for why this
// matters.
import { ConnectionAuthError } from "./errors";

// Default Postgres connect_timeout (seconds), used by the materialization
// storage catalog ATTACH so a slow or wedged libpq handshake fails the
// caller in seconds instead of stalling the worker until the K8s liveness
// probe trips.
//
// libpq enforces a documented minimum of 2 seconds — values below 2
// effectively round up to ~2s wall clock.
export function pgConnectTimeoutSeconds(): number {
   const raw = process.env.PG_CONNECT_TIMEOUT_SECONDS;
   if (!raw) return 5;
   const parsed = Number.parseInt(raw, 10);
   return Number.isFinite(parsed) && parsed > 0 ? parsed : 5;
}

// libpq accepts both keyword=value form ("host=h dbname=d") and URI form
// ("postgresql://u:p@h/d?param=v"). The materialization-storage catalogUrl
// can also arrive as `postgres:<keyword=value>` (no `//`). We detect URI
// form (with `//`) so we know whether to append a new parameter using
// `?`/`&` or a leading space.
const URI_FORM_RE = /^[a-z][a-z0-9+.-]*:\/\//i;

// Match an existing connect_timeout key in either form. URI form uses
// `?key=` or `&key=`; keyword form uses whitespace separation or start-of-
// string. Without the `[?&]` alternatives a URI-form user-supplied timeout
// would be missed and we'd double-append, producing an invalid URL.
const HAS_CONNECT_TIMEOUT_RE = /[?&\s]connect_timeout=|^connect_timeout=/;

// Append `connect_timeout=N` to a libpq-compatible connection string if
// the caller hasn't already set one. Handles keyword form ("host=h ..."),
// URI form ("postgresql://..."), and the `postgres:host=h ...` keyword
// form with a scheme prefix used by DuckLake catalogUrls.
export function withPgConnectTimeout(
   connectionString: string,
   timeout: number,
): string {
   if (HAS_CONNECT_TIMEOUT_RE.test(connectionString)) {
      return connectionString;
   }
   if (URI_FORM_RE.test(connectionString)) {
      // URI form: append as query parameter. `?` if no query string yet,
      // `&` otherwise. A bare trailing `?` (empty query) gets no extra
      // separator. We don't try to handle URL fragments — libpq URIs don't
      // use them.
      if (!connectionString.includes("?")) {
         return `${connectionString}?connect_timeout=${timeout}`;
      }
      if (connectionString.endsWith("?")) {
         return `${connectionString}connect_timeout=${timeout}`;
      }
      return `${connectionString}&connect_timeout=${timeout}`;
   }
   // Keyword=value form (with or without `postgres:` scheme prefix).
   return `${connectionString} connect_timeout=${timeout}`;
}

// Redact libpq `password=...` values from a string before it goes into a
// log line or HTTP response body. Handles bare and quoted values.
//
// Scope: keyword-form `password=` only. Does not touch URL-style
// `user:pw@host` credentials, AWS keys, GCS secrets, etc.
export function redactPgSecrets(s: string): string {
   return s.replace(/password=('[^']*'|"[^"]*"|\S+)/gi, "password=***");
}

// Substring-match libpq error patterns that indicate a non-retryable
// auth/permission failure. Returns a ConnectionAuthError when matched so
// callers can fast-fail with HTTP 422 (semantically "the supplied creds
// are bad; don't retry") instead of letting the raw error fall through to
// a generic 500 that retry loops treat as transient.
export function classifyPgError(
   error: unknown,
   context: string,
): ConnectionAuthError | undefined {
   if (!(error instanceof Error)) return undefined;
   const msg = error.message;
   const patterns = [
      /password authentication failed/i,
      /pg_hba\.conf/i,
      /role ".*" does not exist/i,
      /database ".*" does not exist/i,
      /permission denied/i,
   ];
   if (!patterns.some((p) => p.test(msg))) return undefined;
   return new ConnectionAuthError(`${context}: ${redactPgSecrets(msg)}`);
}

// Outcome of inspecting an error thrown by an `ATTACH` call:
//   - `{ action: "swallow" }`: DuckDB reported the db is already attached
//     (idempotent re-attach); caller should log and continue.
//   - `{ action: "throw", error: ConnectionAuthError }`: classified as a
//     non-retryable auth failure; caller should warn-log and throw it.
//   - `{ action: "throw", error: <original> }`: unrecognized; caller
//     should rethrow as-is to preserve the original cause for diagnosis.
//
// Extracted so the decision tree gets a direct unit test without needing
// to stub DuckDB or run a real ATTACH.
export type PgAttachErrorOutcome =
   | { action: "swallow" }
   | { action: "throw"; error: Error };

export function handlePgAttachError(
   error: unknown,
   context: string,
): PgAttachErrorOutcome {
   if (
      error instanceof Error &&
      (error.message.includes("already exists") ||
         error.message.includes("already attached"))
   ) {
      return { action: "swallow" };
   }
   const authErr = classifyPgError(error, context);
   if (authErr) {
      return { action: "throw", error: authErr };
   }
   if (error instanceof Error) {
      return { action: "throw", error };
   }
   // Non-Error thrown values get wrapped so the catch contract stays
   // (always throws an Error).
   return { action: "throw", error: new Error(String(error)) };
}
