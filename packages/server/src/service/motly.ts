// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * MOTLY (Malloy Object Tag Language) parsing primitives.
 *
 * A given's control contract (`# label`, `# control`, `# suggest`) is declared
 * on the `given:` itself rather than on the surface presenting it, so every
 * reader of that contract needs the same parse. This module is that one parse.
 */

import { parseAnnotation, type Tag } from "@malloydata/malloy-tag";

/**
 * Whether an annotation is on the MOTLY route (plain `#`/`##`), separate from
 * whether it should be read: `parseMotly` must tell "not MOTLY" from "MOTLY but
 * dropped for `@env.`" to report the second. Per Malloy's `parsePrefix`, the
 * MOTLY route is the one where nothing remains after stripping the sigil.
 */
export function onMotlyRoute(text: string): boolean {
   const afterSigil = text.replace(/^##?\|?/, "");
   // `[ \t\r\n]`, not `\s`: Malloy's separator class is narrower, and a
   // pasted U+00A0 admitted here would make the parser swallow
   // `# label="Region"` as the route. Malloy drops that line as
   // `malformed-route`, so dropping it here agrees.
   return afterSigil === "" || /^[ \t\r\n]/.test(afterSigil);
}

/**
 * Keep only MOTLY-route annotations that carry no `@env.` reference. Other
 * routes are a different namespace (`#@` persistence, `##!` compiler flags,
 * `#(doc)`/`#"` prose), not merely unparseable.
 *
 * Core's `Annotations.parseAsTag('')` does this job, but would skip
 * {@link quoteFilterLiterals}; swap only together with a fix for that.
 */
export function motlyAnnotations(texts: readonly string[]): string[] {
   return texts.filter((text) => onMotlyRoute(text) && !hasEnvReference(text));
}

/**
 * Whether an annotation contains a MOTLY environment reference.
 *
 * `@env.NAME` is hydrated by the tag parser from the **server's own**
 * `process.env`. These tags are parsed and returned to clients, so an author who
 * can add one line to a `.malloy` file (in the multi-tenant service, a tenant)
 * could otherwise read a worker's connection credentials back out of the model
 * endpoint. An annotation carrying one is therefore dropped before parsing, in
 * {@link motlyAnnotations}, which every reader in this module goes through.
 *
 * That containment stops at this module's edge. `service/build_plan.ts` calls
 * `annotations.parseAsTag("@")` itself, and both of these reach a client:
 *
 * - `deriveAnnotationFields` returns top-level scalars, so
 *   `#@ persist name=@env.SNOWFLAKE_PRIVATE_KEY` comes back in the wire field
 *   `annotationFields` on `BuildPlan` (`Package.buildPlan`).
 * - `tagQueryMetadataLayer` does the same for
 *   `#@ persist queryMetadata { owner=@env.… }`.
 *
 * The `persist` keyword is load-bearing in both repros: without it a source
 * never enters `getBuildPlan().sources` and nothing is read. The braced
 * `#@ persist { name=… }` happens not to reach (`text()` on a block is
 * undefined) — not a mitigation. That path needs its own fix; this guard does
 * not cover it.
 *
 * Deliberately blunt: not quote-aware (`# description="see @env.md"` is dropped
 * too) and it drops the whole line, not one value — a missing control is
 * visible, a leaked credential is not. Detection is exact: `@ENV.` and
 * `@env .` are parse errors, not references. The drop is per line, so
 * `## note=@env.X` on a separate line does not remove an artifact tag. It is
 * reported via {@link ENV_REFERENCE_DROPPED}; otherwise a dashboard whose tag
 * carried one would silently not exist.
 */
export function hasEnvReference(annotation: string): boolean {
   return annotation.includes("@env.");
}

/**
 * Longest annotation this will hand to the parser, and the message for one that
 * is refused.
 *
 * `parseAnnotation` is superlinear in the number of PROPERTIES, not raw length,
 * so bytes are a crude proxy. Measured per annotation: one long quoted value
 * 1.1ms at 63KB; flat `k=v` run 194ms at 92KB; nested braces 32ms at 9KB. The
 * largest annotation in any fixture is 162 characters, so 8KB is ~50x real use
 * while capping the worst shape to tens of milliseconds.
 *
 * It matters because MOTLY parsing runs per request, uncached, on the main
 * event loop (notebook listing and fetch); an unbounded 110KB annotation held
 * the loop for 1.29s, stalling every tenant on the pod.
 *
 * REFUSED, not truncated: a truncated annotation parses into something the
 * author did not write. The underlying tag-parser amplification is upstream.
 */
export const MAX_ANNOTATION_CHARS = 8_192;

export const ANNOTATION_TOO_LONG = `annotation exceeds ${MAX_ANNOTATION_CHARS} characters and was not parsed`;

/**
 * Message used when an annotation was dropped for carrying an `@env.` reference.
 * Reported rather than silent: the drop is upstream of the parse, so without it
 * a dashboard whose artifact tag carries one simply does not exist.
 */
export const ENV_REFERENCE_DROPPED =
   "annotation dropped for carrying an @env. reference";

/**
 * `parseAnnotation`, with the length bound applied and a throw caught.
 *
 * The catch is kept because a parse that throws would otherwise fail the whole
 * package load; the message is passed on so the author sees why.
 *
 * Prototype-pollution safety (`# __proto__ { a=b }`) rests on
 * `@malloydata/motly-ts-parser` 0.9.1 or later, reached through
 * `@malloydata/malloy-tag`; `given.spec.ts` fails if a bump pulls an older one.
 */
export function parseBounded(texts: readonly string[]): {
   tag: Tag | undefined;
   messages: string[];
} {
   if (texts.some((text) => text.length > MAX_ANNOTATION_CHARS)) {
      return { tag: undefined, messages: [ANNOTATION_TOO_LONG] };
   }
   try {
      const result = parseAnnotation([...texts]);
      return {
         tag: result.tag,
         messages: result.log.map((error) => error.message),
      };
   } catch (error) {
      return {
         tag: undefined,
         messages: [error instanceof Error ? error.message : String(error)],
      };
   }
}

/**
 * Malloy's `#"` doc-comment text, which `title` falls back to. `#"` resolves to
 * the `"` route, whose payload is prose rather than MOTLY, so it is read as
 * text: sigil, route sigil, one separator, then the content.
 */
export function docCommentText(texts: readonly string[]): string | undefined {
   const lines = texts
      // `[ \t\r\n]`, not `\s`, for the same reason as `onMotlyRoute`.
      .filter((text) => /^##?\|?"([ \t\r\n]|$)/.test(text))
      .map((text) => text.replace(/^##?\|?"[ \t\r\n]?/, "").trimEnd());
   // Newline, not space: this route is markdown written one `#"` line per
   // source line, so a space would merge a heading into the paragraph below.
   // Blank lines are kept because they separate paragraphs.
   return lines.some((text) => text.trim().length > 0)
      ? lines.join("\n")
      : undefined;
}

/**
 * A doc comment split where a title fallback would cut it: the first non-empty
 * line, and whatever prose follows it.
 *
 * One function so both halves agree on the cut. A title is one line because it
 * renders on one line everywhere; the whole comment would embed newlines.
 */
function splitDocComment(texts: readonly string[]): {
   title?: string;
   body?: string;
} {
   const text = docCommentText(texts);
   if (text === undefined) return {};
   const lines = text.split("\n");
   const titleLine = lines.findIndex((line) => line.trim().length > 0);
   if (titleLine === -1) return {};
   const body = lines
      .slice(titleLine + 1)
      .join("\n")
      // Leading blank lines are the separator between the title line and the
      // paragraph after it, and belong to neither.
      .replace(/^[ \t\r\n]+/, "")
      .trimEnd();
   return {
      title: lines[titleLine].trim(),
      body: body.length > 0 ? body : undefined,
   };
}

/**
 * The title and description a document publishes, resolved together so the
 * doc-comment line a fallback title takes is not repeated as the description.
 * An explicit `title=` consumes none of the comment, which stays the whole
 * description. Every title fallback (notebook listing, composite and
 * single-query dashboard) goes through this. No final fallback here: callers
 * differ (dashboard slug, notebook's first markdown heading).
 */
export function docCommentTitleAndDescription(
   texts: readonly string[],
   explicitTitle: string | undefined,
): { title?: string; description?: string } {
   if (explicitTitle !== undefined) {
      return { title: explicitTitle, description: docCommentText(texts) };
   }
   const { title, body } = splitDocComment(texts);
   return { title, description: body };
}

/**
 * Quote bare filter literals so MOTLY can parse them.
 *
 * Starting values are documented as filter literals
 * (`# artifact { givens { MANUFACTURER=f'Ford Motor Company' } }`), but MOTLY
 * has no filter-literal form. A bare `f'…'` anywhere on a line makes the parser
 * yield an *empty* tag (not undefined, so `if (!tag)` cannot detect it), losing
 * every property on that line — a dashboard written that way would not be
 * discovered at all. Rewriting `=f'…'` to `="f'…'"` keeps the on-disk grammar
 * while staying inside MOTLY. A reader calling `parseAnnotation` directly (the
 * renderer reading `# dashboard { columns }`) is not covered.
 *
 * **Do not call this directly.** {@link parseMotly} applies it only to text
 * MOTLY has already rejected and keeps the result only if it then parses, so a
 * valid annotation is never rewritten.
 *
 * Residual limitation: on a line that is rescued, a delimiter form the walk
 * misses could alter unrelated text and the result would be kept because it
 * parses. Tolerable only because the alternative is losing every property on
 * the line. The delimiter set comes from the grammar ({@link endOfDelimited});
 * adding a missed form there is the fix — do not assume none remain.
 *
 * The rescue re-emits with single quotes, so `f"US"` reads back `f'US'`.
 */
export function quoteFilterLiterals(annotation: string): string {
   // A value can begin after `=`, or after `[` or `,` inside an array; without
   // the array starts, `R=[f'US', f'CA']` costs every sibling property on the
   // line. This does not make array-valued starting givens work:
   // {@link readStartingGivens} skips arrays.
   const VALUE_START = /[=[,]/;
   const BARE_FILTER_LITERAL = /^([ \t]*)f(['"])((?:\\.|(?!\2)[^\\])*)\2/;
   let out = "";
   let i = 0;
   while (i < annotation.length) {
      const char = annotation[i];
      const delimited = endOfDelimited(annotation, i);
      if (delimited > i) {
         out += annotation.slice(i, delimited);
         i = delimited;
         continue;
      }
      if (VALUE_START.test(char)) {
         const match = BARE_FILTER_LITERAL.exec(annotation.slice(i + 1));
         if (match) {
            const body = match[3].replace(/\\/g, "\\\\").replace(/"/g, '\\"');
            out += `${char}${match[1]}"f'${body}'"`;
            i += 1 + match[0].length;
            continue;
         }
      }
      out += char;
      i += 1;
   }
   return out;
}

/**
 * Index just past the delimited region starting at `start`, or `start` itself
 * when nothing is delimited there.
 *
 * The set of delimiters comes from MOTLY's own grammar
 * (`@malloydata/motly-ts-parser/grammar/source.motly.tmGrammar.json`):
 *
 * - `"""…"""` and `'''…'''`, which may contain a bare quote, so a triple
 *   delimiter is matched before a single one.
 * - `"…"` and `'…'`, which end at a newline as well as at the closing quote.
 * - `` `…` ``, a quoted identifier (always a key). MOTLY decodes escapes inside
 *   it, so an escaped newline or backtick is part of the key, unlike what the
 *   grammar's `` `[^`\n]*` `` suggests.
 * - `<<<` … `>>>`, a heredoc whose terminator sits alone on its own line.
 *
 * An unterminated region runs to the end of the text, leaving it for MOTLY to
 * reject rather than letting the caller rewrite it into something that parses
 * but means something else.
 */
function endOfDelimited(text: string, start: number): number {
   const char = text[start];

   if (char === "`") {
      // Pair backslashes: `` # `a\`b`=1 `` is the single key ``a`b``. A bare
      // `indexOf` would stop at the escaped backtick and never scan the rest.
      let i = start + 1;
      while (i < text.length) {
         if (text[i] === "\\") {
            i += 2;
            continue;
         }
         if (text[i] === "`") return i + 1;
         if (text[i] === "\n") return text.length;
         i += 1;
      }
      return text.length;
   }

   if (text.startsWith("<<<", start)) {
      // Walk lines exactly as MOTLY's `parseHeredoc` does: split on `\n` only,
      // end at the first line whose `trim()` is `>>>`. A `/m` regex is NOT
      // equivalent: its anchors also break at `\r`, U+2028 and U+2029, so a
      // decoy `>>>` would end the region early and the caller would rewrite
      // inside what MOTLY still treats as body.
      let lineStart = start + 3;
      for (;;) {
         const newline = text.indexOf("\n", lineStart);
         const lineEnd = newline === -1 ? text.length : newline;
         if (text.slice(lineStart, lineEnd).trim() === ">>>") return lineEnd;
         if (newline === -1) return text.length;
         lineStart = newline + 1;
      }
   }

   if (char === '"' || char === "'") {
      const triple = char.repeat(3);
      if (text.startsWith(triple, start)) {
         // Pair backslashes: ending at an escaped `\"""` would let the caller
         // rewrite inside the body. Over-pairing only runs the region too far,
         // which fails to rescue an already-unparseable line: the safe direction.
         let i = start + triple.length;
         while (i < text.length) {
            if (text[i] === "\\") {
               i += 2;
               continue;
            }
            if (text.startsWith(triple, i)) return i + triple.length;
            i += 1;
         }
         return text.length;
      }
      let i = start + 1;
      while (i < text.length) {
         if (text[i] === "\\") {
            i += 2;
            continue;
         }
         if (text[i] === char) return i + 1;
         if (text[i] === "\n") return i;
         i += 1;
      }
      return text.length;
   }

   return start;
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

/**
 * Parse an entity's MOTLY annotations, rescuing only what needs it.
 *
 * The parser decides, not the rewrite: an annotation that already parses, or
 * whose rewrite does not help, is returned untouched. That makes "the rewrite
 * cannot change the meaning of a valid tag" true by construction rather than
 * by enumerating string forms, which repeatedly missed cases.
 */
export function parseMotly(texts: readonly string[]): {
   tag: Tag | undefined;
   errors: string[];
} {
   // Split rather than filtered once, so the env drop can be reported; the
   // parsed set is identical to what `motlyAnnotations` returns.
   const onRoute = texts.filter(onMotlyRoute);
   const motly = onRoute.filter((text) => !hasEnvReference(text));
   const envDropped = onRoute.length - motly.length;
   const envErrors: string[] = envDropped > 0 ? [ENV_REFERENCE_DROPPED] : [];
   if (motly.length === 0) return { tag: undefined, errors: envErrors };

   // Only a failed whole-set parse pays for the per-line rescue.
   const direct = parseBounded(motly);
   if (direct.messages.length === 0)
      return { tag: direct.tag, errors: envErrors };

   const rescued = motly.map((text) => {
      if (parseBounded([text]).messages.length === 0) return text;
      const rewritten = quoteFilterLiterals(text);
      return parseBounded([rewritten]).messages.length === 0 ? rewritten : text;
   });
   const after = parseBounded(rescued);
   // Keep `envErrors`, so a malformed sibling line cannot hide an env drop.
   return { tag: after.tag, errors: [...envErrors, ...after.messages] };
}

/**
 * Read a tag property as text without letting a bad literal escape.
 *
 * `Tag.text()` can THROW: MOTLY accepts `@2024-13-01`, `hydrate` builds an
 * Invalid Date, and `text()` calls `toISOString()` on it. Callers map over every
 * given without a try/catch, so one typo would fail the whole package load.
 * Dropping the one field is the proportionate outcome. (Near misses such as
 * `@2024-06-31` do not throw; they roll forward.)
 */
export function tagText(
   tag: Tag | undefined,
   ...path: string[]
): string | undefined {
   try {
      return tag?.text(...path);
   } catch {
      return undefined;
   }
}

/**
 * Read a tag property as a finite number.
 *
 * Not `Tag.numeric()`, which is `parseFloat`: it reads `100px` as 100 and
 * overflows to `Infinity`, which `JSON.stringify` renders as `null` against a
 * `type: number` field. Non-numeric and non-finite are treated as absent, the
 * spec's fallback for a missing bound.
 */
export function tagNumeric(
   tag: Tag | undefined,
   ...path: string[]
): number | undefined {
   try {
      const raw = tagText(tag, ...path);
      if (raw === undefined) return undefined;
      // Decimal only, spelled out rather than `Number()`, which takes `0x10`
      // as 16 and `  ` as 0. A bound the author must correct beats one the
      // slider invents. (Slightly wider than MOTLY's bare number: `+5`, `5.`.)
      const DECIMAL = /^[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?$/;
      const text = raw.trim();
      if (!DECIMAL.test(text)) return undefined;
      // `1e999` is decimal and still overflows to Infinity.
      const value = Number(text);
      return Number.isFinite(value) ? value : undefined;
   } catch {
      return undefined;
   }
}

/**
 * The MOTLY tag of an entity's annotations, with filter literals made parseable.
 *
 * A parse failure is not signalled by this return: the loss is per line, so a
 * non-empty tag is not evidence of a clean parse, and a single failing line
 * yields an empty tag, not `undefined`. See {@link motlyParseErrors}' caveats.
 */
export function motlyTag(texts: readonly string[]): Tag | undefined {
   return parseMotly(texts).tag;
}

/**
 * MOTLY errors across a set of annotations, as messages.
 *
 * Narrower than a full well-formedness check:
 *
 * - **Syntax only.** `parseAnnotation` surfaces `MOTLYSession.parse()` errors and
 *   discards `finish()`, which is where MOTLY defers reference resolution. So
 *   `# label=$missing` reports nothing here while the property is dropped.
 * - **Post-rescue.** A line that only parses because of
 *   {@link quoteFilterLiterals} reports clean, even though Malloy and the
 *   renderer both still reject the text as written.
 * - **No position.** Only messages survive.
 *
 * So an empty result is not proof a tag is well formed.
 */
export function motlyParseErrors(texts: readonly string[]): string[] {
   return parseMotly(texts).errors;
}

/**
 * Whether control changes re-run immediately, from an `autorun=` property.
 *
 * Defaults true, and only an explicit `false` batches behind an Apply button;
 * MOTLY renders a bare `autorun=false` as the text `"false"`. Shared so a
 * notebook's `## autorun=false` and a dashboard's
 * `# artifact { autorun=false }` mean the same thing.
 */
export function readAutorun(tag: Tag | undefined): boolean {
   return tagText(tag, "autorun") !== "false";
}

/**
 * A tag value's scalar type, or undefined. Guarded like {@link tagText} so
 * every tag read has one shape.
 */
function scalarTypeOf(tag: Tag | undefined): string | undefined {
   try {
      return tag?.scalarType();
   } catch {
      return undefined;
   }
}

/**
 * The calendar day a MOTLY `date` literal was written as, independent of the
 * server's timezone.
 *
 * Which field set is authoritative depends on how the value was hydrated, and
 * the two forms differ. A bare `@2024-03-01` is hydrated at UTC midnight in
 * every zone, so its UTC fields carry the authored day. An ISO
 * `@2024-03-01T23:30` is hydrated in LOCAL time, so its LOCAL fields do.
 * Exact-UTC-midnight is what tells them apart.
 *
 * Known limitation: an ISO literal that lands exactly on UTC midnight
 * (`@2024-03-01T16:00` in America/Los_Angeles) is indistinguishable from a bare
 * date, since the `Tag` keeps only the hydrated `Date`, so it can be one day
 * off. Fixing it needs the annotation text threaded down here.
 */
function authoredDay(isoText: string): string {
   const value = new Date(isoText);
   if (Number.isNaN(value.getTime())) return isoText.slice(0, 10);
   const atUtcMidnight =
      value.getUTCHours() === 0 &&
      value.getUTCMinutes() === 0 &&
      value.getUTCSeconds() === 0 &&
      value.getUTCMilliseconds() === 0;
   const year = atUtcMidnight ? value.getUTCFullYear() : value.getFullYear();
   const month = atUtcMidnight ? value.getUTCMonth() : value.getMonth();
   const day = atUtcMidnight ? value.getUTCDate() : value.getDate();
   return `${year}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/**
 * Starting values for a document's controls, from a `givens { … }` block.
 *
 * Values come back in the shape the query endpoint takes, so a `filter<…>` is
 * the filter body rather than the `f'…'` literal it is written as (see
 * {@link unwrapFilterLiteral}). URL parameters override these.
 *
 * Shared so a notebook's `## givens { REGION=f'US' }` and a dashboard's
 * `# artifact { givens { REGION=f'US' } }` mean the same thing.
 */
export function readStartingGivens(
   tag: Tag | undefined,
   /**
    * The declared type of a given by name, so a `filter<…>` value can be
    * unwrapped and nothing else touched. Required: without it this cannot tell
    * `NOTE :: string is f'x'`, where `f'x'` IS the value, from
    * `REGION :: filter<string> is f'US'`, where it is a wrapper.
    */
   declaredType: (name: string) => string | undefined,
): Record<string, string> | undefined {
   const entries = tag?.tag("givens");
   if (!entries) return undefined;
   const collected: Record<string, string> = {};
   for (const [name, value] of entries.entries()) {
      const tag = value as Tag;
      const text = tagText(tag);
      if (text === undefined) continue;
      // A MOTLY date literal arrives as a `Date` rendered by `toISOString()`,
      // which the query endpoint refuses for a `date` given (it wants
      // YYYY-MM-DD). Slicing ten characters is NOT enough: an ISO literal like
      // `@2024-03-01T23:30` hydrates in LOCAL time, so the UTC rendering can be
      // a different day depending on the worker's TZ — rebuild the authored day
      // instead. The DECLARED type decides, because `scalarType()` also says
      // "date" for a timestamp literal, and a `timestamp` given must keep its
      // time of day.
      const declared = declaredType(name);
      collected[name] =
         scalarTypeOf(tag) === "date" &&
         declared !== "timestamp" &&
         declared !== "timestamptz"
            ? authoredDay(text)
            : declared?.startsWith("filter<")
              ? unwrapFilterLiteral(text)
              : text;
   }
   return Object.keys(collected).length > 0 ? collected : undefined;
}
