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
