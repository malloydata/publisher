/**
 * MOTLY (Malloy Object Tag Language) parsing primitives.
 *
 * Extracted from `dashboard.ts` because the dashboard grammar is no longer the
 * only reader: a given's control contract (`# label`, `# control`, `# suggest`)
 * is declared on the `given:` itself, so every surface that renders a control
 * needs the same parse. See `given.ts`.
 */

import { parseAnnotation, type Tag } from "@malloydata/malloy-tag";

/**
 * Keep only annotations on Malloy's MOTLY route — the plain `#`/`##` tags the
 * artifact grammar is written in. Everything else (`#(doc)`, `#"`, `##!`, `#@`)
 * is a different namespace whose payload is not MOTLY, and feeding one to the
 * MOTLY parser produces noise at best.
 *
 * Per Malloy's `parsePrefix`, an annotation's prefix runs from the sigil to the
 * first whitespace; the MOTLY route is the one where nothing is left after
 * stripping the sigil. We test that directly rather than reimplementing route
 * classification.
 *
 * TODO: this is `parsePrefix(text).route === ''`, but `@malloydata/malloy` does
 * not export `parsePrefix` from its barrel. Drop this in favour of
 * `Annotations.forRoute('')` once the annotation bundle (rather than the
 * flattened texts) reaches this layer, or once core exports the prefix parser.
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
 * Malloyyo documents per-dashboard starting values as filter literals —
 * `# artifact { givens { MANUFACTURER=f'Ford Motor Company' } }` — but MOTLY
 * has no filter-literal value form: it fails with "Expected an identifier" and,
 * critically, **discards the entire tag**, so a dashboard written the
 * documented way would not merely lose its starting values, it would not be
 * discovered at all. Rewriting `=f'…'` to `="f'…'"` before parsing keeps the
 * on-disk grammar byte-compatible with Malloyyo while staying inside MOTLY.
 *
 * The rewrite cannot change the meaning of an already-valid tag, because a bare
 * `f'…'` value is never valid MOTLY in the first place.
 */
export function quoteFilterLiterals(annotation: string): string {
   return annotation.replace(
      /=\s*f(['"])((?:\\.|(?!\1)[^\\])*)\1/g,
      (_match, _quote: string, body: string) =>
         `="f'${body.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}'"`,
   );
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
   // Parse errors are not surfaced here: an unparseable tag simply yields no
   // artifact, and the file is treated as a shared include. `lintUndiscoveredDashboard`
   // is where an author is told about that, since it is otherwise a silent
   // disappearance.
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
 * `# artifact { autorun=false }` mean the same thing — the property is about
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
 * `# artifact { givens { REGION=f'US' } }` mean the same thing — like
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
