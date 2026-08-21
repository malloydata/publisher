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
 * caller-authored text for both filter inheritance and the query boundary —
 * composition over a queryable source is itself queryable.
 */
export function buildSourceAliasMap(query: string): Map<string, string> {
   const aliasOf = new Map<string, string>();
   const declRe =
      /source\s*:\s*(?:`([^`]+)`|(\w+))\s+is\s+(?:`([^`]+)`|(\w+))/g;
   let match: RegExpExecArray | null;
   while ((match = declRe.exec(query)) !== null) {
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
 * Backtick-quoted identifiers are deliberately PRESERVED: unlike `'…'`/`"…"`
 * they carry real names (`` source: `my-src` is X ``) that the scan must see.
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
 * Deliberately NOT {@link buildSourceAliasMap}, which this does not replace:
 * that one feeds the query BOUNDARY, where an extra edge widens ADMISSION, so
 * it stays exactly as narrow as it has always been. This one feeds the
 * authorize gate, where an extra edge widens DENIAL, so it is built to
 * over-collect on purpose:
 *  - `query:` declarations are included, so a `query:` hop between a
 *    derivation and the `run:` cannot break the chain;
 *  - a name maps to a SET, keeping every base declared for it rather than the
 *    last, so a second (forged or shadowing) declaration can only add a base
 *    to check, never replace the real one.
 *
 * Expects text already passed through {@link stripMalloyCommentsAndLiterals}.
 */
export function buildDerivationBaseMap(text: string): Map<string, Set<string>> {
   const basesOf = new Map<string, Set<string>>();
   const declRe =
      /(?:source|query)\s*:\s*(?:`([^`]+)`|(\w+))\s+is\s+(?:`([^`]+)`|(\w+))/g;
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
