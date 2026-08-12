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
export function motlyAnnotations(texts: readonly string[]): string[] {
   return texts.filter((text) => {
      const afterSigil = text.replace(/^##?\|?/, "");
      // `[ \t\r\n]` rather than `\s`, matching Malloy's separator class exactly.
      // JS `\s` is wider (U+00A0, \f, \v, U+2000, U+3000 and more), so a stray
      // non-breaking space from a copy-paste would be admitted here and then
      // NOT stripped by the parser's own narrower prefix rule, which swallows
      // `# label="Region"` as the route and reads only what follows. The given
      // keeps its `control` and silently loses its `label`. Malloy classifies
      // that line `malformed-route` and drops it, so dropping it here agrees.
      const onMotlyRoute = afterSigil === "" || /^[ \t\r\n]/.test(afterSigil);
      return onMotlyRoute && !hasEnvReference(text);
   });
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
 * `service/build_plan.ts` calls `annotations.parseAsTag("@")` itself, so a
 * `#@ persist { name=@env.SNOWFLAKE_PRIVATE_KEY }` is hydrated and returned in
 * `annotationFields` without passing through here. That path predates this
 * change and is deliberately not touched by it; it needs its own fix, and this
 * guard should not be read as covering it.
 *
 * Deliberately blunt, and it errs on the safe side in two ways. It is not
 * quote-aware, so `# description="see @env.md"` is dropped too even though that
 * text would never hydrate; and it drops the whole annotation rather than the
 * one value, so a legitimate sibling `label=` on the same line goes with it. A
 * missing control is a visible inconvenience, a leaked credential is not, and
 * nothing in this repo writes `@env.` in a tag today. Detection is exact
 * (`@env.` is case-sensitive, and `@ENV.` or `@env .` are parse errors rather
 * than references), so this cannot silently miss the form it guards against.
 */
export function hasEnvReference(annotation: string): boolean {
   return annotation.includes("@env.");
}

/**
 * Every object the tag parser can be tricked into writing into.
 *
 * Computed once from the runtime rather than listed by hand, because listing by
 * hand is what went wrong twice. The parser resolves a property path with
 * `key in properties`, and `in` walks the prototype chain, so a MOTLY key of
 * `__proto__` reaches `Object.prototype` while `constructor` reaches the global
 * `Object` and `toString` reaches the built-in method object. The reachable set
 * is therefore exactly `Object.prototype` plus the value of each of its own
 * properties, which is what this derives. Twelve objects today, and it stays
 * correct if the runtime grows another.
 */
const POLLUTION_TARGETS: readonly object[] = (() => {
   const seen = new Set<object>();
   const add = (value: unknown): void => {
      if (
         (typeof value === "object" || typeof value === "function") &&
         value !== null
      ) {
         seen.add(value as object);
      }
   };
   add(Object.prototype);
   for (const key of Object.getOwnPropertyNames(Object.prototype)) {
      const descriptor = Object.getOwnPropertyDescriptor(Object.prototype, key);
      if (descriptor && "value" in descriptor) add(descriptor.value);
   }
   return [...seen];
})();

/**
 * Message used when an annotation cannot be parsed without collateral damage.
 * Surfaced through {@link motlyParseErrors} so the reader a later slice builds
 * has something to report rather than a silent empty tag.
 */
const UNSAFE_TO_PARSE = "annotation could not be parsed safely";

/**
 * `parseAnnotation`, with any damage it does to the shared prototypes undone.
 *
 * The tag parser writes tag properties into a plain object, so a MOTLY property
 * whose name resolves along that object's prototype chain lands on a shared
 * global instead of in the bag. `# __proto__ { a=b }` puts `location` and
 * `properties` on `Object.prototype` and then throws `RangeError`, after which
 * every later parse throws too; `# __proto__=x` does the same silently with an
 * empty log; and `# constructor { k=v }` writes tenant data onto the global
 * `Object`, which accumulates without bound and which watching `Object.prototype`
 * alone never sees.
 *
 * **This deliberately does not try to recognise the hostile input.** An earlier
 * version refused any annotation containing the substring `__proto__` and was
 * defeated in review, because MOTLY decodes escapes inside a backtick-quoted
 * identifier, so `` # `__prot\o__` { a=b } `` spells the same property with no
 * such substring. Any denylist of spellings invites that.
 *
 * So the effect is observed instead. Every object in {@link POLLUTION_TARGETS} is
 * snapshotted, the parse runs, and any own property gained is deleted; the
 * annotation is then reported unparseable, since its properties are the ones that
 * went astray. Deleting measurably restores both the prototypes and later parses.
 * Safe because `parseAnnotation` is synchronous, so nothing interleaves and the
 * only keys removed are the ones this call added.
 *
 * A repair, not a fix. The fix is upstream, where that bag wants a `Map` or an
 * `Object.create(null)`, and this should be deleted once that lands.
 */
function parseGuarded(texts: readonly string[]): {
   tag: Tag | undefined;
   messages: string[];
} {
   const before = POLLUTION_TARGETS.map((target) =>
      Object.getOwnPropertyNames(target),
   );
   const undoPollution = (): boolean => {
      let polluted = false;
      POLLUTION_TARGETS.forEach((target, index) => {
         for (const key of Object.getOwnPropertyNames(target)) {
            if (before[index].includes(key)) continue;
            polluted = true;
            // Per-key try, because the repair itself must not be able to throw.
            // Modules are strict, so `delete` on a non-configurable property
            // raises a TypeError, and this also runs inside the catch below,
            // where a throw would escape and fail the whole package load: the
            // outcome the guard exists to prevent.
            try {
               delete (target as Record<string, unknown>)[key];
            } catch {
               // Left in place. Reporting the annotation unparseable is still
               // right, and is what the flag below causes.
            }
         }
      });
      return polluted;
   };
   try {
      const result = parseAnnotation([...texts]);
      if (undoPollution())
         return { tag: undefined, messages: [UNSAFE_TO_PARSE] };
      return {
         tag: result.tag,
         messages: result.log.map((error) => error.message),
      };
   } catch {
      undoPollution();
      return { tag: undefined, messages: [UNSAFE_TO_PARSE] };
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
 *   never a value, and it cannot span a newline.
 * - `<<<` … `>>>`, a heredoc whose terminator sits alone on its own line.
 *
 * An unterminated region runs to the end of the text, which leaves it for MOTLY
 * to reject rather than having the caller rewrite it into something that parses
 * but means something else.
 */
function endOfDelimited(text: string, start: number): number {
   const char = text[start];

   if (char === "`") {
      const close = text.indexOf("`", start + 1);
      const newline = text.indexOf("\n", start + 1);
      const unterminated = close === -1 || (newline !== -1 && newline < close);
      // Unterminated runs to the end, like every other branch here. Stopping just
      // past the opening backtick instead would let the caller keep walking
      // inside an unclosed identifier and rewrite there.
      return unterminated ? text.length : close + 1;
   }

   if (text.startsWith("<<<", start)) {
      const terminator = /^[ \t]*>>>[ \t]*$/m.exec(text.slice(start + 3));
      return terminator
         ? start + 3 + terminator.index + terminator[0].length
         : text.length;
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
function parseMotly(texts: readonly string[]): {
   tag: Tag | undefined;
   errors: string[];
} {
   const motly = motlyAnnotations(texts);
   if (motly.length === 0) return { tag: undefined, errors: [] };

   // The common case is one parse of the whole set. Only a failure pays for the
   // per-line rescue below, so an entity whose annotations are all well formed
   // is not charged for a workaround it does not need.
   const direct = parseGuarded(motly);
   if (direct.messages.length === 0) return { tag: direct.tag, errors: [] };

   const rescued = motly.map((text) => {
      if (parseGuarded([text]).messages.length === 0) return text;
      const rewritten = quoteFilterLiterals(text);
      return parseGuarded([rewritten]).messages.length === 0 ? rewritten : text;
   });
   const after = parseGuarded(rescued);
   return { tag: after.tag, errors: after.messages };
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
 * one from this return value. The loss is per LINE, not per entity: an entity
 * whose second annotation line is malformed still gets the first line's
 * properties, so a non-empty tag is not evidence of a clean parse either. With a
 * single failing line the tag comes back empty. {@link motlyParseErrors} is the
 * closest thing to a signal, and read its caveats before relying on it.
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
      const text = tagText(value as Tag);
      if (text !== undefined) collected[name] = unwrapFilterLiteral(text);
   }
   return Object.keys(collected).length > 0 ? collected : undefined;
}
