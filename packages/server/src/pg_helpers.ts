// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
//   3. The keyword-form pass. A single-quoted value is matched whole, escaped
//      quotes included, since libpq conninfo quoting backslash-escapes `'`.
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
      .replace(/password=('(?:\\.|[^'\\])*'|"[^"]*"|\S+)/gi, "password=***");
}

// The secret-bearing field names across every connection type the API accepts,
// as they appear when a driver or a serializer echoes the config it was handed.
// `password` is covered by redactPgSecrets' keyword pass and is not repeated.
//
// Matched case-insensitively and in either casing convention, because the same
// field arrives as `privateKey` from the API schema and `private_key` from a
// driver that snake-cases its config before reporting it.
const SECRET_FIELD_NAMES = [
   "privateKey",
   "privateKeyPass",
   "serviceAccountKeyJson",
   "accessToken",
   "oauthClientSecret",
   "clientSecret",
   "secretAccessKey",
   "sessionToken",
   "peakaKey",
   "sasUrl",
   "connectionString",
   "token",
   "secret",
];

// `name: value`, `name=value` or `"name": "value"`, for each name above. The
// value class stops at the separators that end a field in JSON, in libpq
// keyword form and in ordinary prose, so a match cannot run past the field it
// started in and swallow the rest of the message.
const SECRET_FIELD_PATTERN = new RegExp(
   String.raw`(["']?(?:${SECRET_FIELD_NAMES.map((n) =>
      // Accept camelCase and snake_case for the same field.
      n.replace(/([A-Z])/g, "[_-]?$1"),
   ).join("|")})["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^,;&\s}]+)`,
   "gi",
);

// A PEM block is the one secret shape with no field name in front of it: a
// driver reporting "could not parse key: -----BEGIN RSA PRIVATE KEY----- ..."
// carries the whole key as prose. Matched by its own delimiters instead.
const PEM_BLOCK_PATTERN =
   /-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----/g;

/**
 * Redact every connection credential this server can be handed, from a string
 * that is about to reach a caller or a log.
 *
 * A connection test is given the caller's own configuration and reports why it
 * did not work, and drivers build those messages by quoting the configuration
 * back. {@link redactPgSecrets} covers the Postgres shapes -- URI userinfo and
 * `password=` -- which leaves the credential of every other connection type
 * (SSH keys and their passphrases, service-account JSON, bearer tokens, storage
 * secret keys) in the message verbatim. This covers those, then defers to
 * {@link redactPgSecrets} for the shapes it already handles.
 *
 * Shape-based for the same reason that function is: the input is arbitrary
 * error prose that may embed a serialized config, so there is nothing to parse.
 * Over-redaction is the safe direction and is preferred to a leak -- a field
 * merely named `token` is masked whether or not it held a credential.
 */
export function redactConnectionSecrets(s: string): string {
   return redactPgSecrets(
      s
         .replace(PEM_BLOCK_PATTERN, "***")
         .replace(SECRET_FIELD_PATTERN, "$1***"),
   );
}
