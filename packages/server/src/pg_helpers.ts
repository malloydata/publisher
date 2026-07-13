// Postgres / libpq helper shared between `service/` (user-facing connections)
// and `storage/` (materialization-storage catalog). Lives at `src/` root so
// neither layer takes a dependency on the other — see CLAUDE.md's "Two
// parallel DuckLake/PG attach paths" note for why this matters.

// Redact libpq `password=...` values from a string before it goes into a
// log line or HTTP response body. Handles bare and quoted values.
//
// Scope: keyword-form `password=` only. Does not touch URL-style
// `user:pw@host` credentials, AWS keys, GCS secrets, etc.
export function redactPgSecrets(s: string): string {
   return s.replace(/password=('[^']*'|"[^"]*"|\S+)/gi, "password=***");
}
