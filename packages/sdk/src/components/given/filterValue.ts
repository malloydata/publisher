/**
 * Encoding between control widgets and `filter<…>` given values.
 *
 * A `filter<T>` given takes Malloy filter syntax as a string — `"us-east,
 * us-west"` for a string filter, `">= 100"` for a number one (see
 * `docs/givens.md` §Accepted JS Shapes). A select or a slider works in plain
 * values, so something has to translate, and it is worth keeping that
 * translation here rather than inline in a widget: it is the one part of the
 * control layer that can silently produce a filter meaning something other than
 * what the user picked.
 */

/** True for the `filter<…>` family, whose values are filter syntax, not plain. */
export function isFilterType(type: string | undefined): boolean {
   return (type ?? "").startsWith("filter<");
}

/** The `T` of a `filter<T>`, or undefined for any other type. */
export function filterInnerType(type: string | undefined): string | undefined {
   const match = /^filter<(.+)>$/.exec(type ?? "");
   return match ? match[1] : undefined;
}

/**
 * A value needs quoting inside a filter list when a bare reading of it would
 * parse as something else: a comma would split it in two, and a quote or
 * backslash would break out of the literal.
 */
function needsQuoting(value: string): boolean {
   return /[,"\\]/.test(value) || value.trim() !== value || value === "";
}

/**
 * Join picked values into one string filter (`"Nike, Levi's"`), which Malloy
 * reads as "any of these".
 *
 * An apostrophe is left bare on purpose — it is ordinary inside a filter word
 * and quoting every `Levi's` would be noise. A comma, a double quote, a
 * backslash, or surrounding whitespace does get quoted, since those change how
 * the list parses.
 */
export function encodeFilterList(values: readonly string[]): string {
   return values
      .map((value) =>
         needsQuoting(value)
            ? `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`
            : value,
      )
      .join(", ");
}

/**
 * Split a string filter back into the values a multiselect should show
 * selected, undoing {@link encodeFilterList}.
 *
 * Only round-trips the list form. Anything else a filter can express — `-Nike`,
 * `%foo%`, `null` — comes back as a single opaque entry rather than being
 * reinterpreted, so a hand-written filter is preserved rather than mangled by a
 * control that cannot represent it.
 */
export function decodeFilterList(value: string): string[] {
   const trimmed = value.trim();
   if (trimmed === "") return [];

   const values: string[] = [];
   let current = "";
   let quoted = false;
   let escaped = false;
   // Whitespace is only significant inside quotes, which is the reason it was
   // quoted; an unquoted entry is trimmed so `a, b` reads as two clean values.
   let wasQuoted = false;
   const flush = () => {
      values.push(wasQuoted ? current : current.trim());
      current = "";
      wasQuoted = false;
   };
   for (const char of trimmed) {
      if (escaped) {
         current += char;
         escaped = false;
      } else if (char === "\\") {
         escaped = true;
      } else if (char === '"') {
         quoted = !quoted;
         wasQuoted = true;
      } else if (char === "," && !quoted) {
         flush();
      } else {
         current += char;
      }
   }
   flush();
   return values.filter((entry) => entry !== "");
}

/** A lower-bound number filter, which is what a one-handled slider means. */
export function encodeAtLeast(value: number): string {
   return `>= ${value}`;
}

/** The bound back out of {@link encodeAtLeast}, or undefined if it is some other filter. */
export function decodeAtLeast(value: string): number | undefined {
   const match = /^>=\s*(-?\d+(?:\.\d+)?)$/.exec(value.trim());
   return match ? Number(match[1]) : undefined;
}
