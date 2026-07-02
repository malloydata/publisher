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
   const runMatch = query.match(/run\s*:\s*(?:`([^`]+)`|(\w+))\s*->/);
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
