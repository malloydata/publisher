// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Pure parsing of caller-authored Malloy query text.
 *
 * The authorization gate, filter inheritance, and the query boundary all need
 * to identify the run target and follow `source: NAME is BASE` derivation
 * chains *before* the query compiles — a denied caller must never reach
 * compilation. These helpers are deliberately side-effect free (no model
 * state) so the regexes that back those security checks can be unit tested in
 * isolation from the stateful `Model`.
 *
 * Both helpers recognize a bare `\w+` identifier and a backtick-quoted Malloy
 * identifier (e.g. `customer-orders`, which needs quoting for the hyphen), and
 * return the inner name without backticks so callers can key it the same way
 * sources are keyed.
 */

/**
 * The top-level source a `run:` / `->` query targets, or undefined when the
 * text has no recognizable run target.
 */
export function extractRunTargetSourceName(query?: string): string | undefined {
   if (!query) return undefined;
   // The `run:` form does NOT require a following `->`. A run target can be an
   // expression over the name — `run: locked extend { … } -> { … }`, or a
   // refinement of a named query, `run: locked_q + { … }` — and requiring `->`
   // right after the identifier missed both. Those were the shapes that skipped
   // the pre-compile gate and got their compile errors back (a column-name and
   // column-type oracle on a source the caller is denied on) while the compiled
   // backstop denied them a moment later. Anchoring on `run:` is what keeps this
   // safe to widen: the identifier after it is the run target or nothing.
   const runMatch = query.match(/run\s*:\s*(?:`([^`]+)`|(\w+))/);
   // The bare leading-`->` form still requires the arrow. Without it this would
   // match the first word of any statement (`source`, `query`, …) and resolve a
   // keyword as the run target.
   const arrowMatch = query.match(/^\s*(?:`([^`]+)`|(\w+))\s*->/m);
   return runMatch?.[1] ?? runMatch?.[2] ?? arrowMatch?.[1] ?? arrowMatch?.[2];
}

/**
 * Map each ad-hoc source alias to the base it derives from
 * (`source: NAME is BASE …` → NAME → BASE). Used to walk derivation chains in
 * caller-authored text for filter inheritance -- a filter-protected source
 * carries its filter requirements when read under a derived name.
 *
 * `source:`-only and last-declaration-wins. It reads STRIPPED text, so the two
 * misreads this map used to carry are closed: a declaration spelled inside a
 * string literal is no longer an edge that last-wins could use to REPLACE a
 * real base, and a comment between `is` and the base no longer ERASES one.
 *
 * Those misreads are why the query boundary stopped reading this map: a replaced
 * edge re-pointed a name from the hidden base it really derives from to a
 * curated one and bought admission. That boundary still uses
 * {@link buildDerivationBaseMap}, which is set-valued and refuses on ambiguity;
 * this map stays last-wins and single-valued because
 * {@link Model.resolveFilterSource} needs ONE source name to inject filters
 * from. Prefer that one on any path where an edge grants access.
 *
 * Strips its own input rather than trusting the caller to have done it. A
 * documented "pass me stripped text" precondition would hold only until the
 * next caller, and the failure is silent in the unsafe direction -- a missed
 * edge means no filter is injected, on a path with no post-compile backstop.
 * {@link stripMalloyCommentsAndLiterals} blanks to spaces, so it is idempotent
 * and a caller that already stripped pays a second scan and nothing else.
 */
export function buildSourceAliasMap(query: string): Map<string, string> {
   const aliasOf = new Map<string, string>();
   const text = stripMalloyCommentsAndLiterals(query);
   // The same identifier and declaration shapes {@link buildDerivationBaseMap}
   // reads, for the same reason: each is legal grammar the compiler links and a
   // narrower pattern silently declined to. `\w` is ASCII-only, so `café` went
   // unmatched; a parameter list after the name and a parenthesised base are
   // both ordinary. Missing an edge here is silent in the unsafe direction --
   // no filter is injected and the caller sees unfiltered rows with no error --
   // so the pattern matching the compiler's reading is what the guarantee rests
   // on. Deliberately still `source:`-only and single-valued: this feeds
   // `resolveFilterSource`, which needs exactly one base to inject from.
   const ident = String.raw`(?:\x60([^\x60]+)\x60|([\p{L}\p{N}_]+))`;
   const declRe = new RegExp(
      String.raw`source\s*:\s*${ident}(?:\s*\([^)]*\))?\s+is\s*\(?\s*${ident}`,
      "gu",
   );
   let match: RegExpExecArray | null;
   while ((match = declRe.exec(text)) !== null) {
      aliasOf.set(match[1] ?? match[2], match[3] ?? match[4]);
   }
   return aliasOf;
}

/**
 * `text` with every comment and every STRING-LITERAL BODY blanked out, so a
 * pattern scan over caller-authored Malloy sees only real syntax.
 *
 * Both halves are security-relevant, in opposite directions:
 *  - **Comments** can HIDE a declaration from a scan that a compiler still
 *    reads around (`source: mine is -- c\n X extend { … }`), or plant a decoy
 *    one that never compiles at all (`-- run: bogus`).
 *  - **String literals** can FORGE a declaration. A scan that reads inside
 *    them lets `where: note = 'source: mine is open_src'` inject an alias
 *    edge, which — if a later edge for a name could overwrite an earlier one —
 *    would let a caller relabel its own derivation's base. (The alias map
 *    below keeps EVERY base per name rather than the last, so forging is
 *    additive and cannot erase a real edge; blanking literals closes it at
 *    the source as well.)
 *
 * Comments are recognized outside literals only, and literals outside
 * comments only, in ONE left-to-right pass — a `--` inside a string is not a
 * comment, and a `'` inside a comment does not open a string. Line comments
 * (`--`, `//`) run to the newline; block comments (slash-star) to their close,
 * or to end of input when unterminated.
 *
 * Backtick-quoted identifiers are PRESERVED and SKIPPED WHOLE: unlike
 * `'…'`/`"…"` they carry real names (`` source: `my-src` is X ``) that the scan
 * must see, and Malloy lexes the span as a single token, so nothing inside it
 * can open a comment or a literal. Scanning into one was a bypass in its own
 * right — a legal `` dimension: `q'` is 1 `` opened a phantom literal that
 * blanked every declaration after it.
 *
 * Replacement is space-for-character, so every offset, line, and token
 * boundary in the result matches the input.
 */
export function stripMalloyCommentsAndLiterals(text: string): string {
   const out = text.split("");
   const blank = (from: number, to: number): void => {
      for (let i = from; i < to && i < out.length; i++) {
         if (out[i] !== "\n") out[i] = " ";
      }
   };
   for (let i = 0; i < text.length; i++) {
      const two = text.slice(i, i + 2);
      if (two === "--" || two === "//") {
         const nl = text.indexOf("\n", i);
         const end = nl === -1 ? text.length : nl;
         blank(i, end);
         i = end;
         continue;
      }
      if (two === "/*") {
         const close = text.indexOf("*/", i + 2);
         const end = close === -1 ? text.length : close + 2;
         blank(i, end);
         i = end - 1;
         continue;
      }
      const ch = text[i];
      if (ch === "`") {
         // A backtick-quoted IDENTIFIER is one token to Malloy's lexer, so
         // nothing inside it can open a comment or a string literal. Skipped
         // WHOLESALE rather than scanned: reading into it let a `'`, `--`,
         // `//` or a block-comment opener in a perfectly legal name
         // (`` source: `a'` is … ``, `` dimension: `q'` is 1 ``) blank real
         // syntax that the compiler went on to read — including every
         // declaration AFTER it, which turned an innocuous field name into a
         // full laundering bypass. Contents are preserved for the same reason
         // they were before: a backticked span carries a real name the
         // declaration scan must see.
         const close = text.indexOf("`", i + 1);
         // Unterminated: the compiler will not accept this text either, so
         // there is nothing further worth scanning.
         i = close === -1 ? text.length : close;
         continue;
      }
      if (ch === "'" || ch === '"') {
         // Blank the BODY, keep both delimiters, so the result still parses as
         // a string where one was and no adjacent tokens are glued together.
         let j = i + 1;
         while (j < text.length && text[j] !== ch) {
            // Malloy escapes a quote inside a literal with a backslash; skip
            // the escaped character so it cannot close the literal early.
            if (text[j] === "\\") j++;
            j++;
         }
         blank(i + 1, j);
         i = j;
         continue;
      }
   }
   return out.join("");
}

/**
 * Every base each ad-hoc alias in `text` may derive from — `source: NAME is
 * BASE` and `query: NAME is BASE` — as NAME → set of BASEs.
 *
 * Built to over-collect on purpose:
 *  - `query:` declarations are included, so a `query:` hop between a
 *    derivation and the `run:` cannot break the chain;
 *  - a name maps to a SET, keeping every base declared for it rather than the
 *    last, so a second (forged or shadowing) declaration can only add a base
 *    to check, never replace the real one.
 *
 * Read by BOTH the authorize gate and the query boundary, which want opposite
 * things from it, so the quantifier -- not the map -- is what carries the
 * direction. The authorize gate denies if ANY branch reaches a gated source,
 * so an extra edge widens DENIAL and over-collection is trivially safe. The
 * boundary admits only if EVERY base proves curated, so an extra edge adds an
 * obligation rather than discharging one, and over-collection is safe there
 * too. A future caller that reads this map with an ANY-branch ADMISSION
 * quantifier would invert that and turn a forged edge into a bypass.
 *
 * Does NOT replace {@link buildSourceAliasMap}, which survives for
 * `resolveFilterSource`'s filter-inheritance walk.
 *
 * Strips its own input, for the reason {@link buildSourceAliasMap} gives: a
 * documented precondition holds only until the next caller, and the strip is
 * idempotent, so a caller that already stripped pays a second scan and nothing
 * else. The stakes are lower here than there -- this map is set-valued and its
 * callers refuse on ambiguity rather than picking one -- but the argument for
 * owning the guarantee rather than documenting it is the same.
 */
export function buildDerivationBaseMap(query: string): Map<string, Set<string>> {
   const basesOf = new Map<string, Set<string>>();
   const text = stripMalloyCommentsAndLiterals(query);
   // `\w` is ASCII-only, so `café` matched nothing; identifiers use the
   // Unicode property classes instead. An optional parameter list after the
   // name (`mine(p::string) is …`) and an optional `(` before the base
   // (`is (X extend { … })`) are both read, because both are legal grammar a
   // narrower pattern silently declined to link.
   const ident = String.raw`(?:\x60([^\x60]+)\x60|([\p{L}\p{N}_]+))`;
   const declRe = new RegExp(
      String.raw`(?:source|query)\s*:\s*${ident}(?:\s*\([^)]*\))?\s+is\s*\(?\s*${ident}`,
      "gu",
   );
   let match: RegExpExecArray | null;
   while ((match = declRe.exec(text)) !== null) {
      const name = match[1] ?? match[2];
      const base = match[3] ?? match[4];
      const bases = basesOf.get(name) ?? new Set<string>();
      bases.add(base);
      basesOf.set(name, bases);
   }
   return basesOf;
}
