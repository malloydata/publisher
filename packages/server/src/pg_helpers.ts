// Postgres / libpq helper shared between `service/` (user-facing connections)
// and `storage/` (materialization-storage catalog). Lives at `src/` root so
// neither layer takes a dependency on the other — see CLAUDE.md's "Two
// parallel DuckLake/PG attach paths" note for why this matters.

// Redact Postgres credentials from a string before it goes into a log line
// or HTTP response body. Covers both libpq forms: keyword `password=...`
// (bare and quoted values) and URI userinfo (`scheme://user:pass@host`).
// The input is arbitrary error prose that may carry zero or more connection
// strings: attach failures echo user-supplied connection strings verbatim
// (DuckDB's postgres extension embeds the full DSN), so URL-parsing is not
// an option and substitution has to be shape-based.
//
// Three passes, and the order is load-bearing (URI passes first: the
// keyword pass's \S+ would otherwise eat a URI's `@host/db` tail whenever
// a password contains the text `password=`):
//   1. Any-scheme URI userinfo. The password class ([^/\s]+) is greedy to
//      the LAST `@` before a `/`, matching how WHATWG URL and libpq split
//      userinfo, so a literal `@` in a password redacts fully; bounding at
//      `/` keeps path-@ URLs (https://h:443/@scope/pkg) and comma-joined
//      URI lists from collapsing into one match. The username class allows
//      empty (`postgres://:pw@h`) and a literal `@` (Azure's `user@servername`
//      form, `postgres://myadmin@srv:pw@host`), stopping at the first `:`;
//      it excludes `?#` so a passwordless URI with `:...@` in its query is
//      not mangled by this pass.
//   2. A postgres-scheme-only mop-up with first-@ semantics ([^@\s]+), for
//      a shape pass 1 fails open on: a raw `/` in the password (invalid
//      per RFC 3986, but connectionStrings flow through verbatim). It only
//      recovers the password when no raw `@` precedes the `/`: with both
//      (`postgres://u:p@a/b@h/d`), pass 1 redacts through the first `@`,
//      and its inserted `***@` satisfies this pass's first-`@` before it
//      ever reaches the raw `/`, so the password tail after the raw `@`
//      stays visible (a residual gap, see below). Scheme-restricted so it
//      can never touch an https URL.
//   3. The original keyword-form pass, unchanged.
//
// The scheme has no leading `\b`/anchor on purpose: a scheme abutting a
// word char (`x_postgres://u:pw@h`) should still redact. Over-matching a
// credential-shaped `scheme://user:pass@` token is the safe direction, and
// usernames stay visible (they aid debugging, and the keyword pass keeps
// `user=` too). Some non-secret shapes are over-redacted as a result, which
// is always preferred to a leak: `?password=a&sslmode=b` loses the
// `&sslmode=b` tail, and a passwordless `@`-username URI with a raw `@`
// later in its query/fragment (`postgres://u@srv:5432/db?x=a@b`) has its
// tail replaced by `***`. Residual gaps (accepted): the pass-2 shape above
// (a raw `@` and then a raw `/` in one password, doubly invalid) keeps its
// post-`@` tail; and a raw (unencoded) whitespace inside a password ends
// the match early. A valid URI/libpq string encodes whitespace, and
// dropping the `\s` bound would let a match run across surrounding
// message text.
export function redactPgSecrets(s: string): string {
   return s
      .replace(/([a-z][a-z0-9+.-]*:\/\/[^:/?#\s]*):([^/\s]+)@/gi, "$1:***@")
      .replace(
         /((?:postgres|postgresql):\/\/[^:/?#\s]*):([^@\s]+)@/gi,
         "$1:***@",
      )
      .replace(/password=('[^']*'|"[^"]*"|\S+)/gi, "password=***");
}
