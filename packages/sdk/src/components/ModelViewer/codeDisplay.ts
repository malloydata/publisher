// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Drop the doc annotations from the head of a stored definition slice.
 *
 * The same text is the row's hover (see DocTooltip), so leaving it in the
 * code block would print the description twice and bury the one thing the
 * expansion exists to show — the expression — under prose. It would also keep
 * the wrapper-strip below from matching, since the slice would not start with
 * `name is`.
 *
 * Doc annotations only: `#(index)`, renderer tags and any other `#` annotation
 * stay, because they are part of how the field behaves rather than a description
 * of it. Both Malloy doc forms are recognized — `#(doc) …` and consecutive `#"`
 * lines.
 *
 * The whole leading annotation run is scanned, not just its first lines: both
 * orders are legal Malloy, so stopping at a `#(index)` would leave the docstring
 * below it in the code block AND in the hover — the double-print this exists to
 * prevent.
 */
export function stripDocAnnotations(code: string): string {
   const lines = code.replace(/^\n+/, "").split("\n");
   const kept: string[] = [];
   let first = 0;
   while (first < lines.length) {
      const line = lines[first].trim();
      if (!line.startsWith("#")) break;
      if (!line.startsWith('#"') && !line.startsWith("#(doc)"))
         kept.push(lines[first]);
      first += 1;
   }
   return [...kept, ...lines.slice(first)].join("\n").trim();
}

/**
 * Trim the redundant declaration wrapper off a field's stored Malloy
 * definition for display — but only when the body is a BLOCK.
 *
 * `by_state is { … }` shows its dedented body: the braces and their level of
 * indentation are pure wrapper, and `group_by:` / `aggregate:` describe
 * themselves. An EXPRESSION keeps its wrapper, because stripping it throws
 * away the relationship that makes it a definition — `category_grain is
 * category` rendered as a lone `category` reads as a stray field reference,
 * not as "this field is defined as that one".
 *
 * Anything that doesn't match the expected wrapper (multi-statement slices,
 * renames, exotic forms) is shown verbatim rather than mangled.
 */
export function stripDefinitionWrapper(name: string, code: string): string {
   const raw = stripDocAnnotations(code);
   const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
   // `measure: airport_count is …` / `view: by_state is …` / bare `x is …`.
   const prefix = new RegExp(
      `^(?:(?:dimension|measure|view)\\s*:\\s*)?${escaped}\\s+is\\s+`,
   );
   const body = raw.replace(prefix, "");
   if (body === raw) return raw; // no wrapper — leave verbatim

   // Expressions keep `name is …`; only a block body is unwrapped.
   if (!(body.startsWith("{") && body.endsWith("}"))) return raw;

   const inner = body.slice(1, -1).replace(/^\n/, "").replace(/\s+$/, "");
   // Dedent by the smallest indent of the non-blank lines, so the body sits
   // flush like a top-level block.
   const lines = inner.split("\n");
   const indents = lines
      .filter((l) => l.trim())
      .map((l) => (l.match(/^[ \t]*/) as RegExpMatchArray)[0].length);
   const minIndent = indents.length ? Math.min(...indents) : 0;
   const out = lines
      .map((l) => l.slice(minIndent))
      .join("\n")
      .trim();
   return out || raw;
}

/**
 * A view's whole declaration for display — `view: by_year is { … }`, keyword
 * included, with its tags (`# line_chart`) above it and only the doc text dropped
 * (the row prints that already). A view is read as the thing a model declares, so
 * its Definition shows the declaration rather than just the block's body. The stored
 * slice may start at the name (`by_year is { … }`); the keyword is put back then.
 * Anything that isn't that shape is shown verbatim.
 */
export function viewDeclaration(name: string, code: string): string {
   const raw = stripDocAnnotations(code);
   const lines = raw.split("\n");
   const firstCode = lines.findIndex((l) => !l.trim().startsWith("#"));
   if (firstCode < 0) return raw;
   const head = lines[firstCode];
   if (/^\s*view\s*:/.test(head))
      return dedentContinuation(lines, firstCode).join("\n");
   const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
   if (!new RegExp(`^\\s*${escaped}\\s+is\\b`).test(head)) return raw;
   lines[firstCode] = head.replace(/^(\s*)/, "$1view: ");
   return dedentContinuation(lines, firstCode).join("\n");
}

/**
 * A slice cut out of a model file starts at its name, at column zero, while every
 * line after it keeps the indentation it had nested in its `source … extend { }`.
 * Shift the continuation lines left by the shallowest of them — the level of the
 * closing `}` (and of a pipeline's `-> {`), which is where the declaration started —
 * so the body sits one step in from `view:` and the `}` lines up under it.
 */
function dedentContinuation(lines: string[], first: number): string[] {
   const rest = lines.slice(first + 1).filter((l) => l.trim() !== "");
   if (rest.length === 0) return lines;
   const base = Math.min(...rest.map((l) => l.length - l.trimStart().length));
   if (base === 0) return lines;
   return lines.map((l, i) =>
      i <= first ? l : l.slice(Math.min(base, l.length - l.trimStart().length)),
   );
}
