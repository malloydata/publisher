/**
 * Render a given's `default` (a raw Malloy source literal) for display in the
 * input as placeholder / helper text. Returns undefined when there is no
 * default to show.
 *
 * The server surfaces `Given.default` exactly as written in the model
 * (`'WN'`, `2003`, `@2024-01-01`, `f'WN'`). For display we unquote string
 * literals, drop the date `@` sigil, and unwrap filter literals so the user
 * sees `WN` / `2024-01-01` rather than the raw source form. Non-string scalars
 * (number, boolean) and anything we don't special-case are shown verbatim.
 *
 * This only affects what the user *sees* — the value stays empty, so leaving
 * the field blank still means "use the model default" (the server fills it in).
 */
export function renderGivenDefault(
   type: string | undefined,
   rawDefault: string | undefined,
): string | undefined {
   if (!rawDefault) return undefined;
   const literal = rawDefault.trim();
   if (!literal) return undefined;

   const givenType = type ?? "string";

   if (
      givenType === "date" ||
      givenType === "timestamp" ||
      givenType === "timestamptz"
   ) {
      // `@2024-01-01` -> `2024-01-01`
      return literal.replace(/^@/, "");
   }

   if (givenType.startsWith("filter<")) {
      // `f'WN'` -> `WN`: drop the filter-literal `f` prefix, then unquote.
      return unquoteStringLiteral(literal.replace(/^f/, ""));
   }

   if (givenType === "string") {
      return unquoteStringLiteral(literal);
   }

   // number, boolean, array, or unknown: show the literal as authored.
   return literal;
}

/**
 * Strip the surrounding single quotes from a Malloy string literal and undouble
 * any escaped quotes (`''` -> `'`). Leaves a non-quoted input untouched.
 */
function unquoteStringLiteral(literal: string): string {
   if (
      literal.length >= 2 &&
      literal.startsWith("'") &&
      literal.endsWith("'")
   ) {
      return literal.slice(1, -1).replace(/''/g, "'");
   }
   return literal;
}
