// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
 * artifact grammar is written in.
 *
 * The others are excluded because they are a different NAMESPACE, not because
 * their payload is unparseable. Some of them are MOTLY too: `service/build_plan.ts`
 * runs the parser over `#@` and `##!` on purpose. Reading them here would answer
 * a question about the control contract with a tag written about persistence or a
 * compiler flag, and `#(doc)` and `#"` carry prose rather than properties.
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
/**
 * Whether an annotation is on the MOTLY route, separate from whether it should
 * be read. One definition, because `parseMotly` needs to tell "not MOTLY" from
 * "MOTLY but dropped for `@env.`" in order to report the second, and a second
 * copy of the route rule is how these drift.
 */
export function onMotlyRoute(text: string): boolean {
   const afterSigil = text.replace(/^##?\|?/, "");
   // `[ \t\r\n]` rather than `\s`, matching Malloy's separator class exactly.
   // JS `\s` is wider (U+00A0, \f, \v, U+2000, U+3000 and more), so a stray
   // non-breaking space from a copy-paste would be admitted here and then NOT
   // stripped by the parser's own narrower prefix rule, which swallows
   // `# label="Region"` as the route and reads only what follows. The given
   // keeps its `control` and silently loses its `label`. Malloy classifies that
   // line `malformed-route` and drops it, so dropping it here agrees.
   return afterSigil === "" || /^[ \t\r\n]/.test(afterSigil);
}

export function motlyAnnotations(texts: readonly string[]): string[] {
   return texts.filter((text) => onMotlyRoute(text) && !hasEnvReference(text));
}

/**
 * Whether an annotation contains a MOTLY environment reference.
 *
 * `@env.NAME` is hydrated by the tag parser from the **server's own**
 * `process.env` (`@malloydata/malloy-tag`'s `hydrate`, which does
 * `process.env[pv.eq.env]`). That is safe while a plain `#` tag stays inside the
 * server, which is how it was until these tags started being read and returned:
 * `malloyGivenToApi` drops reserved routes from `annotations`, so
 * `# label=@env.PGPASSWORD` never reached a caller. Now that the control
 * contract is derived and shipped, an author who can add one line to a `.malloy`
 * file could otherwise read a worker's connection credentials back out of the
 * model endpoint. In the multi-tenant service that author is a tenant.
 *
 * So an annotation carrying one is dropped before it is parsed, which is why
 * this sits in {@link motlyAnnotations}: every reader *in this module* goes
 * through there, and a value hydrated from the environment is never something a
 * client asked for.
 *
 * That containment stops at this module's edge, and the gap is not hypothetical.
 * `service/build_plan.ts` calls `annotations.parseAsTag("@")` itself, so a value
 * is hydrated and returned without passing through here. **Both spellings below
 * are returned to a client**, so neither is the lesser one to fix:
 *
 * - `deriveAnnotationFields` iterates the *top level* and calls `text()`, so
 *   `#@ persist name=@env.SNOWFLAKE_PRIVATE_KEY` comes back as
 *   `{name: "<the key>"}`. That map is the wire field `annotationFields` on
 *   `BuildPlan`, reachable as `Package.buildPlan`.
 * - `tagQueryMetadataLayer` does the same inside `queryMetadata`, so
 *   `#@ persist queryMetadata { owner=@env.… }` hydrates too, and `queryMetadata`
 *   is likewise declared in `api-doc.yaml`.
 *
 * Both examples keep the `persist` keyword, and that is load-bearing rather than
 * decoration: a source without it never enters `getBuildPlan().sources`, so
 * neither reader is ever called on it and the annotation leaks nothing. An
 * earlier version of this comment dropped it from both examples, which turns a
 * real leak into a repro that yields an empty build plan.
 *
 * The one spelling that does NOT reach is the *braced* `#@ persist { name=… }`:
 * `text()` on a nested block is undefined, so the scalar loop skips it. Treat
 * that as a curiosity, not a mitigation. It appears nowhere in this repo, while
 * the sibling form above is what all 156 occurrences in the fixtures, examples
 * and docs use. An earlier version of this comment cited the braced form as
 * proof `#@ persist` is safe, which is the exact inversion of the truth, in a
 * paragraph whose whole job is scoping a credential-exposure surface.
 *
 * All of it predates this change and is deliberately not touched by it; it needs
 * its own fix, and this guard should not be read as covering it.
 *
 * Deliberately blunt, and it errs on the safe side in two ways. It is not
 * quote-aware, so `# description="see @env.md"` is dropped too even though that
 * text would never hydrate; and it drops the whole annotation rather than the
 * one value, so a legitimate sibling `label=` on the same line goes with it. A
 * missing control is a visible inconvenience, a leaked credential is not, and
 * nothing in this repo writes `@env.` in a tag today. Detection is exact
 * (`@env.` is case-sensitive, and `@ENV.` or `@env .` are parse errors rather
 * than references), so this cannot silently miss the form it guards against.
 *
 * The drop is per LINE, not per file, and that is worth stating because the
 * blast radius reads worse than it is. `## artifact { tiles=[…] }` on one line
 * and `## note=@env.X` on another still produces the dashboard; only an `@env.`
 * on the SAME line as the artifact tag removes it. Measured both ways.
 *
 * Reported since the dashboards slice: the drop is upstream of the parse, so
 * without {@link ENV_REFERENCE_DROPPED} a dashboard whose tag carries one would
 * simply not exist, with no finding anywhere.
 */
export function hasEnvReference(annotation: string): boolean {
   return annotation.includes("@env.");
}

/**
 * Longest annotation this will hand to the parser, and the message for one that
 * is refused.
 *
 * `parseAnnotation` is superlinear in the number of PROPERTIES, not in raw
 * length, which is why this is a crude bound rather than a precise one.
 * Measured in this worktree, one annotation:
 *
 *   one long quoted value   1.1ms at 63KB    (linear, harmless)
 *   flat `k=v` run          194ms at 92KB    (quadratic)
 *   nested braces           32ms at 9KB      (worst per byte)
 *
 * So bytes are a proxy for the thing that actually costs, and a generous one:
 * the largest annotation across every fixture in this repo is 162 characters,
 * so 8KB is roughly fifty times the real ceiling while capping the worst shape
 * to tens of milliseconds.
 *
 * It matters because this slice moved MOTLY parsing onto paths that had none.
 * `getNotebookListing` runs per request for every notebook in a package and
 * `getNotebookModel` per request for one, both uncached, and discovery runs on
 * the main process rather than in the load worker. Measured before this bound:
 * 1.29s of uninterruptible event loop for a single notebook request carrying a
 * 110KB annotation. Publisher is one event loop, so that is not a slow request,
 * it is every tenant on the pod waiting.
 *
 * REFUSED, not truncated. A truncated annotation parses into something the
 * author did not write, which is worse than not parsing at all.
 *
 * The underlying amplification is upstream and not fixed here: Malloy compiles
 * the same 630KB source in 14ms while the tag parser takes 7.49s. Filed
 * separately.
 */
export const MAX_ANNOTATION_CHARS = 8_192;

export const ANNOTATION_TOO_LONG = `annotation exceeds ${MAX_ANNOTATION_CHARS} characters and was not parsed`;

/**
 * Message used when an annotation was dropped for carrying an `@env.` reference.
 *
 * Fixed text, like {@link ANNOTATION_TOO_LONG}, because the annotation is never
 * parsed and there is nothing else honest to say about it. Reported rather than
 * dropped in silence: the guard is upstream of the parse, so without this a
 * dashboard whose artifact tag carries one simply does not exist, with no
 * finding anywhere. That was tolerable while the cost was a missing control; it
 * is not once the cost is the whole dashboard.
 */
export const ENV_REFERENCE_DROPPED =
   "annotation dropped for carrying an @env. reference";

/**
 * `parseAnnotation`, with the length bound applied and a throw caught.
 *
 * The catch is kept because a parse that throws would otherwise fail the whole
 * package load, and the message is passed on so the author sees why.
 *
 * Until Malloy 0.0.434 this also snapshotted every object reachable from
 * `Object.prototype` and undid whatever a parse added there, because the tag
 * parser stored properties in a plain object and `# __proto__ { a=b }` wrote
 * onto the shared prototype. 0.0.434 fixed that in the parser itself, for the
 * `#` routes read here and for the `##!` and `#@` routes the compiler reads, so
 * the snapshot was removed. The pollution test in `given.spec.ts` stays, now
 * pointed at the parser.
 *
 * That safety now rests on `@malloydata/motly-ts-parser` 0.9.1 or later, which
 * we reach only through `@malloydata/malloy-tag`. A dependency bump that pulls
 * an older parser brings the pollution back, and `given.spec.ts` will fail.
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
      // `[ \t\r\n]` for the same reason as motlyAnnotations above: JS `\s` also
      // matches U+00A0 and friends, which Malloy's prefix rule does not, so a
      // pasted non-breaking space would make this read a doc comment the
      // compiler classifies malformed-route and contributes nothing for.
      .filter((text) => /^##?\|?"([ \t\r\n]|$)/.test(text))
      .map((text) => text.replace(/^##?\|?"[ \t\r\n]?/, "").trimEnd());
   // Newline, not space: core defines this route as doc-string markdown and
   // authors write one `#"` line per source line, so joining with a space merges
   // a heading into the paragraph below it. Blank lines are kept for the same
   // reason, being what separates one paragraph from the next.
   return lines.some((text) => text.trim().length > 0)
      ? lines.join("\n")
      : undefined;
}

/**
 * A doc comment split where a title fallback would cut it: the first non-empty
 * line, and whatever prose follows it.
 *
 * One function because the two halves have to agree about where the cut is. The
 * rule "a title is the first non-empty line" lives here and nowhere else, so a
 * caller that takes the title cannot disagree with a caller that takes the rest.
 *
 * Why a line rather than the whole comment: {@link docCommentText} joins with
 * newlines on purpose, since that route carries markdown and authors write one
 * line per source line, whereas a title is rendered on one line everywhere, so
 * the whole comment as a title fallback published an embedded newline.
 */
function splitDocComment(texts: readonly string[]): {
   title?: string;
   body?: string;
} {
   const text = docCommentText(texts);
   if (text === undefined) return {};
   const lines = text.split("\n");
   const titleLine = lines.findIndex((line) => line.trim().length > 0);
   // `docCommentText` returns undefined when every line is blank, so a comment
   // that got here has one. Guarded anyway rather than indexed with -1.
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
 * The title and description a document publishes, resolved together.
 *
 * Separately, they printed the same words twice. `description` was the whole doc
 * comment and `title` fell back to its first line, so a one-line `#" Orders by
 * region` with no `title=` produced a heading and a subtitle reading identically,
 * which is what the dashboard page rendered. Deriving both here means the line
 * the title took is the line the description does not repeat.
 *
 * An explicit `title=` changes that: it did not consume any of the comment, so
 * the comment stays the description in full.
 *
 * Every title fallback in the codebase goes through this, so the two cannot
 * drift: there are three of them (a notebook listing, a composite dashboard, a
 * single-query dashboard) and fixing only the first is how this was a defect
 * twice. Neither field gets a final fallback here, because the callers do not
 * share one: a dashboard falls back to its slug, a notebook to its first
 * markdown heading.
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
 * New in this change, carried across from #935 rather than fixed in place: there
 * is no version of this on `main`, so nothing here is a regression in a shipped
 * release. The defects described below were found and fixed while extracting it.
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
 * **Do not call this directly.** {@link parseMotly} applies it only to text
 * MOTLY has already rejected, and keeps the result only if it then parses, so an
 * annotation that is already valid is never handed to it whatever forms it uses.
 *
 * That structure exists because this walk shipped wrong three times, growing from
 * single quotes to triple quotes to backtick keys and heredoc bodies, each form
 * found after the previous fix was called complete. It now takes its delimiter
 * set from the grammar instead (see {@link endOfDelimited}).
 *
 * **The residual limitation, stated plainly rather than waved away.** Parse-first
 * protects an annotation that parses. It does NOT make this rewrite safe on one
 * that does not: a line rescued because of a bare `f'…'` in one place is rewritten
 * everywhere the walk thinks a value starts, so a delimiter form the walk still
 * misses would alter unrelated text on that same line and the result would be
 * kept, because it parses. That is a real defect class, not a cosmetic one, and
 * the only reason it is tolerable is that the alternative for such a line is
 * losing every property on it. Adding a form to {@link endOfDelimited} is the
 * fix; asserting there are none left is what went wrong three times.
 *
 * A fourth was found in security review and fixed rather than absorbed into this
 * paragraph: the heredoc terminator was matched with a `/m` regex, whose anchors
 * break at `\r`, U+2028 and U+2029 where MOTLY breaks only at `\n`, so a decoy
 * `>>>` after one of those ended the region early and the rescue rewrote inside
 * what MOTLY still treated as body. Measured, pinned by a test that fails against
 * the old regex. How many forms remain is still unknown, which is the whole point
 * of this paragraph; one fewer is not none.
 *
 * One spelling detail, since it is visible on the wire: the rescue always
 * re-emits with single quotes, so an author who wrote `f"US"` reads back `f'US'`.
 * The body is preserved and the value means the same thing, but that one spelling
 * is not a byte-for-byte round-trip.
 */
export function quoteFilterLiterals(annotation: string): string {
   // A bare literal is a value, so it is only looked for where a value can
   // begin: after `=`, and after `[` or `,` inside an array. Anchoring on `=`
   // alone left `givens { R=[f'US', f'CA'] }` unparseable, which cost every
   // sibling property on that line, `dashboard { columns=12 }` included. That
   // recovery is the point. It does NOT make array-valued starting givens work:
   // {@link readStartingGivens} reads scalar text and skips an array either way,
   // and `EncodedGivenValues` has no encoding for one.
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
 * (`@malloydata/motly-ts-parser/grammar/source.motly.tmGrammar.json`) rather than
 * from cases turned up in review, which is how three earlier versions of this
 * walk each shipped missing a form:
 *
 * - `"""…"""` and `'''…'''`, which may contain a bare quote of their own, so a
 *   triple delimiter has to be matched before a single one.
 * - `"…"` and `'…'`, which the grammar ends at a newline as well as at the
 *   closing quote (`"|(?=$)`).
 * - `` `…` ``, a quoted identifier. The grammar notes it is always a key and
 *   never a value. Its `` `[^`\n]*` `` says it cannot span a newline, and that
 *   is the one place the grammar does not describe the parser: MOTLY decodes
 *   escapes inside the identifier, so `` `a\<newline>` `` parses to the single
 *   key `"a\n"`, checked against the parser. The branch below therefore has to
 *   consume an escaped newline rather than treat it as unterminated, which the
 *   backslash skip does. (The skip and the newline bail test the same character,
 *   so their relative order does not matter; a mutation swapping them changes
 *   nothing, which is how this note got corrected.)
 * - `<<<` … `>>>`, a heredoc whose terminator sits alone on its own line.
 *
 * An unterminated region runs to the end of the text, which leaves it for MOTLY
 * to reject rather than having the caller rewrite it into something that parses
 * but means something else.
 */
function endOfDelimited(text: string, start: number): number {
   const char = text[start];

   if (char === "`") {
      // MOTLY decodes escapes inside a quoted identifier, so an escaped backtick
      // is part of the key and not its close: `` # `a\`b`=1 `` parses to the
      // single key ``a`b``, checked against the parser rather than assumed. The
      // grammar writes the identifier as `` `[^`\n]*` ``, which cannot express
      // that, and a bare `indexOf` inherits the same limit: it stops at the
      // escaped backtick, so the rest of the line is never scanned and a filter
      // literal after it is never quoted. That costs the whole annotation, not
      // one key, since the line then stays unparseable and every field with it.
      // Pair backslashes the way the quote branches below do.
      let i = start + 1;
      while (i < text.length) {
         if (text[i] === "\\") {
            i += 2;
            continue;
         }
         if (text[i] === "`") return i + 1;
         // Unterminated runs to the end, like every other branch here. Stopping
         // just past the opening backtick instead would let the caller keep
         // walking inside an unclosed identifier and rewrite there.
         if (text[i] === "\n") return text.length;
         i += 1;
      }
      return text.length;
   }

   if (text.startsWith("<<<", start)) {
      // Walk lines the way MOTLY's `parseHeredoc` does: split on `\n` only, and
      // end at the first line whose `trim()` is `>>>`. A `/^[ \t]*>>>[ \t]*$/m`
      // regex looks equivalent and is not, because JS multiline anchors also
      // break at `\r`, U+2028 and U+2029. It would find a terminator MOTLY does
      // not, end the region early, and let the caller rewrite inside what MOTLY
      // still treats as body. Measured, `# a=<<<\nbody\r>>>\r x=f'US'\n...`
      // had `x=f'US'` quoted inside the heredoc. `trim()` rather than `[ \t]*`
      // for the same reason: it is what MOTLY compares with, and it accepts
      // wider whitespace. Matching its rule also matches the `\n` test the
      // backtick branch above already uses.
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
         // Pair backslashes here as well as in the single-quoted branch below. A
         // bare indexOf ends the region at an escaped `\"""`, which puts the
         // caller back inside the string body and lets it rewrite there, and the
         // rewritten line still parses so the damage is kept. Pairing when the
         // form does not actually support escapes only ever runs the region on
         // too far, and the cost of that is failing to rescue a line that was
         // already unparseable, which is the direction to err in.
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
 * The parser decides, not the rewrite. An annotation that already parses is
 * returned untouched, and one whose rewrite does not help is returned untouched
 * too, so {@link quoteFilterLiterals} only ever sees text MOTLY has already
 * rejected. That is what makes "the rewrite cannot change the meaning of a valid
 * tag" true by construction rather than by enumerating string forms, and
 * enumerating them is what failed: this walk was corrected three times, for
 * quoted values, then triple-quoted ones, then backtick-quoted keys, and each
 * time the next form was found by someone else. MOTLY's own grammar is the
 * authority on what parses, so it is asked instead of imitated.
 */
export function parseMotly(texts: readonly string[]): {
   tag: Tag | undefined;
   errors: string[];
} {
   // Split rather than filtered once, so the env drop can be REPORTED. The set
   // handed to the parser is byte-identical to what `motlyAnnotations` returns;
   // this only recovers which lines it removed, and why.
   const onRoute = texts.filter(onMotlyRoute);
   const motly = onRoute.filter((text) => !hasEnvReference(text));
   const envDropped = onRoute.length - motly.length;
   const envErrors: string[] = envDropped > 0 ? [ENV_REFERENCE_DROPPED] : [];
   if (motly.length === 0) return { tag: undefined, errors: envErrors };

   // The common case is one parse of the whole set. Only a failure pays for the
   // per-line rescue below, so an entity whose annotations are all well formed
   // is not charged for a workaround it does not need.
   const direct = parseBounded(motly);
   if (direct.messages.length === 0)
      return { tag: direct.tag, errors: envErrors };

   const rescued = motly.map((text) => {
      if (parseBounded([text]).messages.length === 0) return text;
      const rewritten = quoteFilterLiterals(text);
      return parseBounded([rewritten]).messages.length === 0 ? rewritten : text;
   });
   const after = parseBounded(rescued);
   // `envErrors` carried through here too. Returning only `after.messages` lost
   // the env drop whenever a SIBLING line was malformed, so one bad annotation
   // hid the fact that another had been dropped entirely.
   return { tag: after.tag, errors: [...envErrors, ...after.messages] };
}

/**
 * Read a tag property as text without letting a bad literal escape.
 *
 * `Tag.text()` can THROW rather than return undefined: MOTLY accepts a date
 * literal such as `@2024-13-01`, `hydrate` builds an Invalid Date from it, and
 * `text()` calls `toISOString()` on that. Both production callers map over every
 * given with no try/catch (`package_load_worker.ts:632` and `:818`), so an
 * unguarded read turns one typo in one tag into a failed load for the whole
 * package, reported as nothing more than the engine's own RangeError text. Do not
 * grep for a fixed string: that is "Invalid Date" on Bun (the Docker CMD and
 * `start:dev`) and "Invalid time value" on Node (the published bin). Dropping the
 * one field is the proportionate outcome. Easy to miss because near misses do not
 * throw: `@2024-06-31` rolls forward to July 1 instead.
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
 * `Tag.numeric()` is `parseFloat` and rejects only NaN, so a bound past
 * `Number.MAX_VALUE` comes back as `Infinity`, which `JSON.stringify` renders as
 * `null` against a field the spec declares `type: number`. A client typed
 * `rangeMax?: number` would then receive null rather than undefined and build a
 * slider from it. Non-finite is treated as absent, which is the fallback the
 * spec already defines for a missing bound.
 */
export function tagNumeric(
   tag: Tag | undefined,
   ...path: string[]
): number | undefined {
   try {
      const raw = tagText(tag, ...path);
      if (raw === undefined) return undefined;
      // `Tag.numeric()` is parseFloat, which reads `100px` as 100 and `12abc` as
      // 12, so a bound that is not a number silently became one and, with its
      // pair present, turned a plain input into a slider bounded by a value
      // nobody wrote.
      //
      // The accepted set is spelled out rather than delegated to `Number()`,
      // which would also take `0x10` as 16 and `  ` as 0. Decimal only,
      // with an optional sign and exponent. That is slightly WIDER than what
      // MOTLY parses as a bare number: it accepts a leading `+` and a trailing
      // bare `.`, which MOTLY rejects outright, so those only arrive here through
      // a quoted value such as `range_max="+5"`. Accepting them is harmless; the
      // point is to reject text that merely begins with digits. `0x10` is absent
      // rather than either 16 or the 0 parseFloat gave it: a bound the author has
      // to correct beats one the slider invents.
      const DECIMAL = /^[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?$/;
      const text = raw.trim();
      if (!DECIMAL.test(text)) return undefined;
      // Still range-check: `1e999` is decimal and overflows to Infinity, which
      // `JSON.stringify` renders as null against a `type: number` field.
      const value = Number(text);
      return Number.isFinite(value) ? value : undefined;
   } catch {
      return undefined;
   }
}

/**
 * The MOTLY tag of an entity's annotations, with filter literals made parseable.
 *
 * A parse failure never shows up as `undefined` here, so a caller cannot detect
 * one from this return value. For an ordinary parse failure the loss is per LINE
 * rather than per entity: an entity whose second annotation line is malformed
 * still gets the first line's properties, so a non-empty tag is not evidence of a
 * clean parse either. With a single failing line the tag comes back empty.
 * {@link motlyParseErrors} is the closest thing to a signal, and read its caveats
 * before relying on it.
 */
export function motlyTag(texts: readonly string[]): Tag | undefined {
   return parseMotly(texts).tag;
}

/**
 * MOTLY errors across a set of annotations, as messages.
 *
 * **Read what this does and does not cover before building on it.** It is the
 * intended input for the package warning a later slice will raise, and it is
 * narrower than that job wants:
 *
 * - **Syntax only.** `parseAnnotation` surfaces `MOTLYSession.parse()` errors and
 *   discards `finish()`, which is where MOTLY defers reference resolution. So
 *   `# label=$missing` reports nothing here while the property is dropped.
 * - **Post-rescue.** A line that only parses because of
 *   {@link quoteFilterLiterals} reports clean, even though Malloy and the
 *   renderer both still reject the text as written.
 * - **No position.** Only messages survive, so a warning built from this cannot
 *   yet say which annotation on which entity is at fault.
 *
 * Each of those is a case where a tag is malformed and this returns nothing, so
 * do not treat an empty result as proof a tag is well formed.
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
 * `# artifact { autorun=false }` mean the same thing. The property is about
 * the cost of re-running, which is a property of the queries rather than of
 * the surface presenting them.
 */
export function readAutorun(tag: Tag | undefined): boolean {
   return tagText(tag, "autorun") !== "false";
}

/**
 * A tag value's scalar type, or undefined. Guarded like {@link tagText}: this
 * one does not stringify, so it cannot throw on a bad literal, but it is kept
 * beside its sibling so every read of a tag goes through one shape.
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
 * One residual, stated rather than hidden: an ISO literal that lands exactly on
 * UTC midnight (`@2024-03-01T16:00` in America/Los_Angeles) is indistinguishable
 * from a bare date at this layer, because the `Tag` keeps only the hydrated
 * `Date` and not the source text, so it takes the UTC day and can be one day
 * off. Closing that needs the literal, which means threading the annotation
 * text down here. Every other case is exact.
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
 * `# artifact { givens { REGION=f'US' } }` mean the same thing. Like
 * `autorun`, where a document starts is a property of the document, not of the
 * surface presenting it.
 */
export function readStartingGivens(
   tag: Tag | undefined,
   /**
    * The declared type of a given by name, so a `filter<…>` value can be
    * unwrapped and nothing else touched. Required rather than optional: without
    * it this cannot tell `NOTE :: string is f'x'`, where `f'x'` IS the value,
    * from `REGION :: filter<string> is f'US'`, where it is a wrapper. Unwrapping
    * blindly published `x` for the first and posted it back as a filter.
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
      // A MOTLY date literal arrives here as a `Date`, and `Tag.text()` renders
      // one with `toISOString()`. That is precisely the spelling the query
      // endpoint REFUSES for a `date` given, with "…YYYY-MM-DD…", so publishing
      // it would hand a caller a starting value that 400s when sent straight
      // back. `# artifact { givens { SINCE=@2024-03-01 } }` is the natural
      // spelling too, since it matches the declaration's own default.
      //
      // `scalarType` is the discriminator rather than a shape test on the
      // string, so a caller who deliberately quoted a full timestamp keeps
      // exactly what they wrote.
      //
      // Taking the first ten characters of that ISO string is NOT sufficient, and
      // an earlier version of this comment claimed it was, on the grounds that
      // `@2024-03-01 10:00` is a parse error so only a bare date can reach here.
      // The space form is indeed refused, but the ISO form `@2024-03-01T23:30`
      // is accepted, still reports `scalarType() === "date"`, and hydrates in
      // LOCAL time before being rendered as UTC. Measured across three zones:
      //
      //   literal                 TZ                   rendered        sliced
      //   @2024-03-01             any                  ...T00:00Z      2024-03-01  ok
      //   @2024-03-01T23:30       America/Los_Angeles  2024-03-02T07:30Z  2024-03-02  WRONG DAY
      //   @2024-03-01T00:30       Asia/Tokyo           2024-02-29T15:30Z  2024-02-29  WRONG DAY
      //
      // Nothing reports it, because the tag parses: a dashboard just opens on
      // the wrong day depending on the worker's TZ. So the day is rebuilt from
      // calendar fields rather than sliced off the UTC rendering.
      // The DECLARED type decides, not the tag's scalar type. `scalarType()`
      // reports "date" for `@2024-03-01T10:30:15` too, so keying on it dropped
      // the time of day from a `timestamp` given's starting value, silently.
      // `declaredType` is in hand, so use it and let a timestamp keep its text.
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
