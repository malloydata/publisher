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
//
// The two URI passes are scanners rather than regular expressions, because
// both regex forms are quadratic on input an attacker controls (the message
// echoes a user-supplied connection string). Pass 1's scheme has no anchor, so
// a backtracking engine retries it at every position of a long run of letters,
// rescanning the rest of the run each time. Pass 2's password class crosses
// `/`, so each of many `postgres://` occurrences with no `@` rescans to the end
// of the message. Each pass below matches exactly what its regex
// (`[a-z][a-z0-9+.-]*://[^:/?#\s]*:[^/\s]+@` and
// `(?:postgres|postgresql)://[^:/?#\s]*:[^@\s]+@`, case-insensitive) matched,
// in one left-to-right pass. Measured on 40,000 repeated letters: 1.6s for the
// pass-1 regex, under 1ms for its scanner. The keyword pass is linear and
// stays a regex.
export function redactPgSecrets(s: string): string {
   return redactUserinfoPasswords(
      redactUserinfoPasswords(s, ANY_SCHEME_LAST_AT),
      PG_SCHEME_FIRST_AT,
   ).replace(/password=('(?:\\.|[^'\\])*'|"[^"]*"|\S+)/gi, "password=***");
}

// One URI-userinfo pass: which schemes it accepts and where the password ends.
interface UserinfoPass {
   // Whether a scheme this pass accepts ends at `sep` (the index of its `://`)
   // and starts at or after `floor`, the end of the previous match.
   hasScheme(s: string, sep: number, floor: number): boolean;
   // Sticky; reads the run of characters a password may contain.
   passwordRun: RegExp;
   // The index of the `@` that ends a password starting at `from` whose run
   // ends at `stop`, or -1 when the userinfo carries no password.
   passwordEnd(s: string, from: number, stop: number): number;
}

// This and each pass's `passwordRun` are sticky, so a read starts exactly at
// `lastIndex`. Each is a single greedy class with nothing after it, so a read
// never backtracks and costs the length of the run it returns.
const USERNAME_RUN = /[^:/?#\s]*/y;

const SCHEME_CHAR = /[a-z0-9+.-]/i;
const SCHEME_LETTER = /[a-z]/i;
const PG_SCHEMES = ["postgres", "postgresql"];

// Pass 1. A scheme is a letter and then letters, digits, `+`, `.` or `-`, so a
// `://` has one iff the run of scheme characters before it holds a letter: the
// first such letter is where the regex's leftmost match began. The password
// runs to the LAST `@` before a `/`, and holds at least one character.
const ANY_SCHEME_LAST_AT: UserinfoPass = {
   hasScheme(s, sep, floor) {
      for (let i = sep - 1; i >= floor && SCHEME_CHAR.test(s[i]); i--) {
         if (SCHEME_LETTER.test(s[i])) return true;
      }
      return false;
   },
   passwordRun: /[^/\s]*/y,
   passwordEnd(s, from, stop) {
      const at = s.slice(from, stop).lastIndexOf("@");
      return at > 0 ? from + at : -1;
   },
};

// Pass 2. The scheme is exactly `postgres` or `postgresql`, and the password
// runs to the FIRST `@`, across `/`, and holds at least one character.
// `toLowerCase` agrees with the regex's case-insensitive match on these two
// words: the only non-ASCII characters that lowercase to ASCII letters become
// `i` or `k`, which neither contains.
const PG_SCHEME_FIRST_AT: UserinfoPass = {
   hasScheme(s, sep, floor) {
      return PG_SCHEMES.some(
         (scheme) =>
            sep - scheme.length >= floor &&
            s.slice(sep - scheme.length, sep).toLowerCase() === scheme,
      );
   },
   passwordRun: /[^@\s]*/y,
   passwordEnd(s, from, stop) {
      return stop > from && s[stop] === "@" ? stop : -1;
   },
};

function runEnd(run: RegExp, s: string, from: number): number {
   run.lastIndex = from;
   run.exec(s); // A run may be empty, so this always matches.
   return run.lastIndex;
}

// Replace each `scheme://user:password@` password with `***`, leaving the
// scheme and username as they were.
function redactUserinfoPasswords(s: string, pass: UserinfoPass): string {
   let out = "";
   // Everything before `copied` is already in `out`; no match starts before it.
   let copied = 0;
   // Where the last password run read stopped. `from` only increases and no
   // stop character lies between an earlier `from` and its stop, so for any
   // `from` at or before it, it is still the first stop. Reusing it is what
   // keeps pass 2 linear: its run crosses `/`, so many `postgres://`
   // occurrences can share one run.
   let stop = -1;
   let sep = s.indexOf("://");
   while (sep !== -1) {
      const userEnd = runEnd(USERNAME_RUN, s, sep + 3);
      let at = -1;
      if (s[userEnd] === ":" && pass.hasScheme(s, sep, copied)) {
         const from = userEnd + 1;
         if (stop < from) stop = runEnd(pass.passwordRun, s, from);
         at = pass.passwordEnd(s, from, stop);
      }
      if (at === -1) {
         sep = s.indexOf("://", sep + 1);
         continue;
      }
      out += s.slice(copied, userEnd + 1) + "***@";
      copied = at + 1;
      sep = s.indexOf("://", copied);
   }
   return out + s.slice(copied);
}

// The secret-bearing field names across every connection type the API accepts,
// as they appear when a driver or a serializer echoes the config it was handed.
//
// `password` is included even though redactPgSecrets has a `password=` pass:
// that pass only matches the libpq `=` form, so a serialized config reporting
// `"password":"..."` in JSON goes straight through it.
//
// Matched case-insensitively and in either casing convention, because the same
// field arrives as `privateKey` from the API schema and `private_key` from a
// driver that snake-cases its config before reporting it.
const SECRET_FIELD_NAMES = [
   "password",
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

// A field assignment carrying one of the names above, in the three forms a
// driver or a serializer produces: `"name": "value"` (JSON), `name=value`
// (libpq keyword and query-string), and `name: value` where the value is
// quoted.
//
// A BARE `name: value` is deliberately NOT matched. Drivers write prose like
// `Cannot parse privateKey: Unsupported key format`, where the text after the
// colon is the explanation rather than the key -- matching it would redact the
// diagnosis and leave the caller with `Cannot parse privateKey: *** key
// format`. Quoting, or an `=`, is what distinguishes a value from a sentence.
const NAME_ALTERNATION = SECRET_FIELD_NAMES.map((n) =>
   // Accept camelCase and snake_case for the same field.
   n.replace(/([A-Z])/g, "[_-]?$1"),
).join("|");

// Quoted value after `:` or `=`, e.g. "privateKey": "..." or privateKey='...'.
//
// Escape-aware in both quote styles, matching the form redactPgSecrets already
// uses for single quotes. A naive `"[^"]*"` ends the match at the first escaped
// quote INSIDE the value, so a credential containing one kept its tail:
// `"password":"ab\"TAIL"` redacted `ab\` and left `TAIL"` in the message.
const SECRET_QUOTED_PATTERN = new RegExp(
   String.raw`(["']?(?:${NAME_ALTERNATION})["']?\s*[:=]\s*)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')`,
   "gi",
);

// Bare value after `=` only. The value class stops at the separators that end a
// field in libpq keyword form and in ordinary prose, so a match cannot run past
// the field it started in.
//
// `&` is NOT a terminator. A URL-valued field carries its credential in a query
// parameter -- an Azure SAS URL's secret IS the `sig=` parameter, which follows
// one -- so stopping at `&` redacted the harmless prefix and left the signature
// in place. The cost of including it is that a bare `a=1&b=2` pair following a
// secret-named field is swallowed whole, which is the safe direction.
const SECRET_BARE_PATTERN = new RegExp(
   String.raw`(["']?(?:${NAME_ALTERNATION})["']?\s*=\s*)(?:[^,;\s}"']+)`,
   "gi",
);

// A PEM block is the one secret shape with no field name in front of it: a
// driver reporting "could not parse key: -----BEGIN RSA PRIVATE KEY----- ..."
// carries the whole key as prose. Matched by its own delimiters instead.
//
// Two patterns rather than one with an alternation. A single pattern offering
// "terminated OR run to the end" lets the engine retry the same input down two
// branches, which is polynomial on a message carrying many BEGIN markers and is
// what CodeQL's js/polynomial-redos flags. Applied in order, each is linear.
//
// The terminated form forbids a further BEGIN inside its body, so two keys in
// one message match as two blocks rather than one span -- and the lazy body has
// no overlapping alternative to backtrack through. Measured on a message of
// 20,000 repeated BEGIN markers: 3ms, against 404ms for the lazy-body form
// without the guard.
const PEM_TERMINATED_PATTERN =
   /-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----(?:(?!-----BEGIN )[\s\S])*?-----END [A-Z0-9 ]*PRIVATE KEY-----/g;

// Whatever BEGIN marker survives the pass above is unterminated: the driver
// truncated the value. A truncated key still contains most of the key, so the
// tail goes with it.
const PEM_UNTERMINATED_PATTERN =
   /-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*$/;

/**
 * Redact every connection credential this server can be handed, from a string
 * that is about to reach a caller or a log, by the SHAPE the credential takes.
 *
 * <p>Named for the shape rather than the concern because
 * {@code materialization_service.redactConnectionSecrets} is the value-based
 * counterpart: given the config in hand, it removes those exact strings. Prefer
 * that one where the config is available -- it needs no list of field names and
 * cannot miss a field the schema gains. This one is the backstop for what it
 * cannot see: an echo that is base64- or URL-encoded, or a secret that reached
 * the message from somewhere other than the config passed in.
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
export function redactConnectionSecretShapes(s: string): string {
   return redactPgSecrets(
      s
         .replace(PEM_TERMINATED_PATTERN, "***")
         .replace(PEM_UNTERMINATED_PATTERN, "***")
         .replace(SECRET_QUOTED_PATTERN, "$1***")
         .replace(SECRET_BARE_PATTERN, "$1***"),
   );
}
