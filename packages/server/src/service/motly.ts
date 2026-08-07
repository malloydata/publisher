/**
 * MOTLY (Malloy Object Tag Language) parsing primitives.
 *
 * A given's control contract (`# label`, `# control`, `# suggest`) is declared
 * on the `given:` itself rather than on the surface presenting it, so every
 * reader of that contract needs the same parse. This module is that one parse.
 * `given.ts` is its first caller; the dashboard reader is the next.
 */

import { parseAnnotation, type Tag } from "@malloydata/malloy-tag";

/**
 * Keep only annotations on Malloy's MOTLY route, the plain `#`/`##` tags the
 * artifact grammar is written in. Everything else (`#(doc)`, `#"`, `##!`, `#@`)
 * is a different namespace whose payload is not MOTLY, and feeding one to the
 * MOTLY parser produces noise at best.
 *
 * Per Malloy's `parsePrefix`, an annotation's prefix runs from the sigil to the
 * first whitespace; the MOTLY route is the one where nothing is left after
 * stripping the sigil. We test that directly rather than reimplementing route
 * classification.
 *
 * TODO: this is `parsePrefix(text).route === ''`, which `@malloydata/malloy`
 * does not export from its barrel, so it is re-derived here. Core already ships
 * the whole job as `Annotations.parseAsTag('')`, and the one production caller
 * (`malloyGivenToApi`) is holding the routed `Annotations` bundle already, so
 * that swap is not blocked on anything; it is left out of the extraction only
 * because `parseAsTag` would skip {@link quoteFilterLiterals} and reintroduce
 * the bare-filter-literal failure below. Take it together with a fix for that.
 */
export function motlyAnnotations(texts: readonly string[]): string[] {
   return texts.filter((text) => {
      const afterSigil = text.replace(/^##?\|?/, "");
      return afterSigil === "" || /^\s/.test(afterSigil);
   });
}

/**
 * Malloy's `#"` doc-comment text, which `title` falls back to. `#"` resolves to
 * the `"` route, whose payload is prose rather than MOTLY, so it is read as
 * text: sigil, route sigil, one separator, then the content.
 */
export function docCommentText(texts: readonly string[]): string | undefined {
   const lines = texts
      .filter((text) => /^##?\|?"(\s|$)/.test(text))
      .map((text) => text.replace(/^##?\|?"\s?/, "").trim())
      .filter((text) => text.length > 0);
   return lines.length > 0 ? lines.join(" ") : undefined;
}

/**
 * Quote bare filter literals so MOTLY can parse them.
 *
 * Malloyyo documents per-dashboard starting values as filter literals, as in
 * `# artifact { givens { MANUFACTURER=f'Ford Motor Company' } }`, but MOTLY has
 * no filter-literal value form. A bare `f'…'` fails with "Expected an
 * identifier", and the failure is not local to the value: the parser yields an
 * *empty* tag rather than no tag, so every property on that annotation line is
 * lost with it. `# artifact { givens { REGION=f'US' } } dashboard { columns=12 }`
 * loses `columns` as well, and a dashboard written the documented way would not
 * merely lose its starting values, it would not be discovered at all. Note the
 * empty-not-absent part: a caller cannot detect this with `if (!tag)`.
 *
 * The trigger is only a bare `f'…'` somewhere on the line. Neither nesting nor
 * the position of a nested block matters, both checked against the parser: the
 * same line with `REGION="US"` keeps `columns=12` even though the block is not
 * last, and a top-level `given=f'US'` fails with nothing nested at all.
 *
 * Rewriting `=f'…'` to `="f'…'"` before parsing keeps the on-disk grammar
 * byte-compatible with Malloyyo while staying inside MOTLY, so every reader
 * going through {@link motlyTag} is covered. One calling `parseAnnotation`
 * directly is not, which is why the renderer reading `# dashboard { columns }`
 * off that same line never sees it.
 *
 * The rewrite only fires outside quoted values, which is what keeps it from
 * changing the meaning of an already-valid tag. A bare `f'…'` is never valid
 * MOTLY, but the same characters *inside* a string are ordinary data: MOTLY
 * takes both `"…"` and `'…'` with backslash escapes, so
 * `# description="Set when status=f'open'"` parses cleanly and must be left
 * alone. A plain regex sweep cannot tell those apart and corrupts the second
 * case into an unparseable line, taking the whole tag with it, so this walks
 * the string instead.
 */
export function quoteFilterLiterals(annotation: string): string {
   const BARE_FILTER_LITERAL = /^=\s*f(['"])((?:\\.|(?!\1)[^\\])*)\1/;
   let out = "";
   let i = 0;
   while (i < annotation.length) {
      const char = annotation[i];
      if (char === '"' || char === "'") {
         const end = endOfQuoted(annotation, i);
         out += annotation.slice(i, end);
         i = end;
         continue;
      }
      if (char === "=") {
         const match = BARE_FILTER_LITERAL.exec(annotation.slice(i));
         if (match) {
            const body = match[2].replace(/\\/g, "\\\\").replace(/"/g, '\\"');
            out += `="f'${body}'"`;
            i += match[0].length;
            continue;
         }
      }
      out += char;
      i += 1;
   }
   return out;
}

/**
 * Index just past the quoted value starting at `start`. An unterminated quote
 * runs to the end, which leaves the text for MOTLY to reject rather than having
 * this rewrite it into something that parses but means something else.
 */
function endOfQuoted(text: string, start: number): number {
   const quote = text[start];
   let i = start + 1;
   while (i < text.length) {
      if (text[i] === "\\") {
         i += 2;
         continue;
      }
      if (text[i] === quote) return i + 1;
      i += 1;
   }
   return text.length;
}

/**
 * A given value written as a filter literal reduces to its body, because that
 * is what the query endpoint takes for a `filter<…>` given (`"us-east, us-west"`,
 * not `f'us-east, us-west'`). Non-filter values pass through untouched.
 */
export function unwrapFilterLiteral(value: string): string {
   const match = /^f(['"])([\s\S]*)\1$/.exec(value);
   return match ? match[2] : value;
}

/** The MOTLY tag of an entity's annotations, with filter literals made parseable. */
export function motlyTag(texts: readonly string[]): Tag | undefined {
   const motly = motlyAnnotations(texts).map(quoteFilterLiterals);
   if (motly.length === 0) return undefined;
   // A parse failure returns an EMPTY tag, not undefined, so a caller cannot
   // detect it from this return value alone (see quoteFilterLiterals above).
   // The errors are not lost: {@link motlyParseErrors} returns them for the
   // same input. Nothing reports them yet, so a malformed tag is silent today;
   // the reader that turns them into a package warning arrives with the
   // dashboard slice, and that is the place to wire it rather than here, since
   // this module has no warning channel of its own.
   return parseAnnotation(motly).tag;
}

/** MOTLY parse errors across a set of annotations, as messages. */
export function motlyParseErrors(texts: readonly string[]): string[] {
   const motly = motlyAnnotations(texts).map(quoteFilterLiterals);
   if (motly.length === 0) return [];
   return parseAnnotation(motly).log.map((error) => error.message);
}

/**
 * Whether control changes re-run immediately, from an `autorun=` property.
 *
 * Defaults true, and only an explicit `false` batches behind an Apply button;
 * MOTLY renders a bare `autorun=false` as the text `"false"`. Shared so a
 * notebook's `## autorun=false` and a dashboard's
 * `# artifact { autorun=false }` mean the same thing. The property is about
 * the cost of re-running, which is a property of the queries rather than of
 * the surface presenting them.
 */
export function readAutorun(tag: Tag | undefined): boolean {
   return tag?.text("autorun") !== "false";
}

/**
 * Starting values for a document's controls, from a `givens { … }` block.
 *
 * Values come back in the shape the query endpoint takes, so a `filter<…>` is
 * the filter body rather than the `f'…'` literal it is written as (see
 * {@link unwrapFilterLiteral}). URL parameters override these.
 *
 * Shared so a notebook's `## givens { REGION=f'US' }` and a dashboard's
 * `# artifact { givens { REGION=f'US' } }` mean the same thing. Like
 * `autorun`, where a document starts is a property of the document, not of the
 * surface presenting it.
 */
export function readStartingGivens(
   tag: Tag | undefined,
): Record<string, string> | undefined {
   const entries = tag?.tag("givens");
   if (!entries) return undefined;
   const collected: Record<string, string> = {};
   for (const [name, value] of entries.entries()) {
      const text = value.text();
      if (text !== undefined) collected[name] = unwrapFilterLiteral(text);
   }
   return Object.keys(collected).length > 0 ? collected : undefined;
}
