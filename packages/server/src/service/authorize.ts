// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(authorize)` / `#(access_filter)` annotation parsing.
 *
 * The body is an UNQUOTED Malloy expression over declared givens (`$NAME`),
 * e.g. `#(authorize) $ROLE = 'analyst'`, taken verbatim off the note and
 * compiled by Malloy in the target source's field scope (its grammar is
 * `authorize_grammar.ts`). A body opening with a double quote is the LEGACY
 * string form and is refused ({@link assertNoLegacyStringGate}).
 *
 * A file-level `##(authorize)` is a load error
 * ({@link assertNoMisplacedAuthorizeAnnotations}, `"file"` kind) naming the
 * remedy: declare `#(authorize)` on each `source:`. The `##` spelling is still
 * RECOGNIZED so it is refused clearly rather than silently ignored.
 *
 * **What counts as the tag is Malloy's answer, not a regex of ours.** A note is
 * a gate iff Malloy routes it to `authorize` ({@link AUTHORIZE_ROUTE}, the
 * lock) or to `access_filter` ({@link ACCESS_FILTER_ROUTE}, the row filter)
 * ({@link noteRoute}), which admits the block form `#|(authorize)` and the
 * other bracket pairs (`#[authorize]`, `#<authorize>`, `#{authorize}`, and the
 * same four for `access_filter`) and excludes near misses like
 * `# (authorize)` (route `''`, Malloy's reserved MOTLY namespace) and
 * `#( authorize )` (malformed prefix). Publisher must not decide any of that for
 * itself: honouring a spelling Malloy routes elsewhere cements a divergence from
 * the compiler, and missing one Malloy DOES route here serves every row from a
 * source its author locked. The near misses are refused outright at load
 * ({@link collectAuthorizeNearMisses}) rather than either honoured or ignored.
 *
 * This module parses, collects, and (at load time) validates gates; it does
 * not ENFORCE them (`Model.authorizeAndBindRunnable` grafts the filter). Every
 * probe it builds is COMPILED, never RUN — the package-load worker's
 * `ProxyConnection.runSQL` deliberately throws. It bundles into that worker,
 * so keep its imports to `@malloydata/malloy`, `module`, `../errors`,
 * `./annotations` and `./authorize_routes`.
 */

import { payloadOf, routeOf } from "@malloydata/malloy";
import { createRequire } from "module";
import { BadRequestError, ModelCompilationError } from "../errors";
import { type AnnotationNote } from "./annotations";
import {
   canonicalAuthorizeRoute,
   RECOGNIZED_AUTHORIZE_SPELLINGS,
   ACCESS_FILTER_ROUTE,
   AUTHORIZE_ROUTE,
} from "./authorize_routes";

/**
 * Malloy's own routing for ONE note, via the public `routeOf`. Three outcomes,
 * and all three are load-bearing here:
 *
 *   a name (`"authorize"`, `"authorize-v2"`, `"doc"`)  an app's namespace
 *   `""`                                              MOTLY / render tags
 *   `undefined`                                        a malformed prefix
 *
 * Reading the compiler's answer rather than text-matching is what keeps
 * publisher from inventing its own grammar — see this module's header.
 */
function noteRoute(text: string): string | undefined {
   return routeOf({ value: text.trimStart() } as Parameters<typeof routeOf>[0]);
}

/** The note's payload — the part after the prefix, dedented for a block note. */
function notePayload(text: string): string {
   return (
      payloadOf({ value: text.trimStart() } as Parameters<
         typeof payloadOf
      >[0]) ?? ""
   );
}

/**
 * A caller-text pattern for an `authorize` annotation, and deliberately a
 * SUPERSET of the spellings that are actually gates.
 *
 * The invariant is ASYMMETRIC, and getting the direction wrong is what makes one
 * side a security hole:
 *
 *   gate spellings  ==  Malloy's `authorize` route  (ask the compiler)
 *   this pattern    ⊇   gate spellings              (a superset, by construction)
 *
 * A rejecter that accepts MORE than the parser is a 400 on odd caller input,
 * which is safe. One that accepts LESS is a forged-gate bypass. They cannot
 * share a definition: {@link assertNoCallerAuthorizeAnnotation} reads raw
 * PRE-COMPILE text where no route exists yet, so a regex finds candidates and
 * Malloy's lexer decides which sit in prose.
 *
 * Covers sigil `##?`, optional block `|`, any of Malloy's four bracket pairs,
 * case-insensitively. `[ \t]`, never `\s`: `\s` matches a newline, making
 * `"# \n(authorize) rules apply"` a spurious 400 on prose. The lookahead
 * requires the name to END there, so other apps' routes (`#(authorized)`,
 * `#(authorize-v2)`, `#(authorize.audit)`) are not refused.
 *
 * Both stems, because a forged gate on EITHER route replaces the base's own
 * under per-route own-wins; both separators and the retired `row`/`source`
 * prefixes, because the pattern must stay a superset of every spelling
 * {@link authorizeAnnotationRoute} treats as a gate (pinned by tests).
 */
export const AUTHORIZE_TAG_LIKE = String.raw`##?\|?[ \t]*(?:[([{<][ \t]*)?(?:(?:(?:row|source)[-_]?)?authorize|access[-_]?filter)(?=[)\]}>]|[ \t]|$)`;
const AUTHORIZE_ANNOTATION_ANYWHERE = new RegExp(AUTHORIZE_TAG_LIKE, "iu");

/**
 * Every spelling that reads as an author reaching for `route`, case-insensitive
 * — the exact route word, its ordinary hyphen/underscore/no-separator variants,
 * and (for a compound route name) the same set with its two words transposed.
 * Transposed spellings like `#(authorize-source)` are distinct Malloy routes,
 * but must fail the load rather than load inert as somebody else's route.
 *
 * The lock's bare-word entry, matched case-insensitively, is what refuses
 * `#(AUTHORIZE)` / `#(Authorize)` / `# (authorize)` — distinct routes to
 * Malloy.
 *
 * No entry may be an empty list ({@link nearMissWordAlternation} throws): an
 * empty alternation matches EVERY route-`undefined` note (`#percent`,
 * `#currency`, `#drill`), failing loads over gates nobody wrote.
 *
 * The retired `row_authorize` / `source_authorize` spellings are near misses
 * of their replacements, so old models fail the load naming the new route.
 */
function nearMissRouteNames(route: string): readonly string[] {
   switch (route) {
      case AUTHORIZE_ROUTE:
         return [
            "authorize",
            "source_authorize",
            "source-authorize",
            "sourceauthorize",
            "authorize_source",
            "authorize-source",
            "authorizesource",
         ];
      case ACCESS_FILTER_ROUTE:
         return [
            "access_filter",
            "access-filter",
            "accessfilter",
            "filter_access",
            "filter-access",
            "filteraccess",
            "row_authorize",
            "row-authorize",
            "rowauthorize",
            "authorize_row",
            "authorize-row",
            "authorizerow",
         ];
      default:
         return [route];
   }
}

const REGEXP_SPECIAL = /[.*+?^${}()|[\]\\]/g;

/** `nearMissRouteNames(route)`, escaped and `|`-joined. Throws on an empty
 *  list — see {@link nearMissRouteNames}. */
function nearMissWordAlternation(route: string): string {
   const names = nearMissRouteNames(route);
   if (names.length === 0) {
      throw new Error(
         `No near-miss spellings for authorize route \`${route}\` — an empty ` +
            "alternation matches every malformed-prefix note, which would " +
            "fail package loads over annotations nobody wrote.",
      );
   }
   return names.map((name) => name.replace(REGEXP_SPECIAL, "\\$&")).join("|");
}

/**
 * A MOTLY (route `""`) payload that is an author reaching for `route`: optional
 * space/tab, an opening bracket, the route word, a closing bracket. The
 * REQUIRED bracket keeps ordinary render tags (`# bar_chart`, `# size=lg`) and
 * an unbracketed MOTLY tag named `authorize` out.
 */
function motlyAuthorizePayloadPattern(route: string): RegExp {
   return new RegExp(
      `^[ \\t]*[([{<][ \\t]*(?:${nearMissWordAlternation(route)})[ \\t]*[)\\]}>]`,
      "iu",
   );
}

/**
 * A malformed-prefix note (route `undefined`) that was reaching for `route` —
 * `#( authorize )`, `#(authorize )`, `#(authorize)X`, `#authorize`.
 *
 * A text match is safe here only BECAUSE the route is already `undefined`: it
 * cannot steal a route that belongs to somebody else.
 */
export function malformedAuthorizeAttemptPattern(route: string): RegExp {
   return new RegExp(
      `^##?\\|?[ \\t]*(?:[([{<][ \\t]*)?(?:${nearMissWordAlternation(route)})`,
      "iu",
   );
}

/**
 * The gate payload on ONE annotation note, or `undefined` if the note is
 * routed to neither `#(authorize)` nor `#(access_filter)`. Malloy's own
 * routing decides — see {@link noteRoute}. A malformed prefix routes to
 * `undefined`, so it is never a gate here. The ONE route test every gate
 * reader shares; the route is reported for callers that tell them apart.
 */
function authorizeNoteContent(
   text: string,
): { route: string; content: string } | undefined {
   const route = canonicalAuthorizeRoute(noteRoute(text));
   if (route === undefined) return undefined;
   return { route, content: notePayload(text) };
}

/** The canonical route of ONE note (`access_filter` / `authorize`), or
 *  `undefined` if it is not a gate. Unlike {@link parseAuthorizeAnnotation},
 *  never throws on an empty body.
 */
export function authorizeAnnotationRoute(text: string): string | undefined {
   return authorizeNoteContent(text)?.route;
}

/** Note routes whose payload the compiler reads as prose. */
const PROSE_ROUTES = new Set(["markdown", "text"]);

interface LexedToken {
   type: number;
   startIndex: number;
   stopIndex: number;
}

interface ErrorCounter {
   errors: number;
   syntaxError(): void;
}

type MakeMalloyParser = (
   code: string,
   options: {
      lexerErrorListener: ErrorCounter;
      parserErrorListener: ErrorCounter;
   },
) => {
   tokenStream: {
      getTokens(): LexedToken[];
      tokenSource?: {
         vocabulary?: {
            getSymbolicName?: (type: number) => string | undefined;
         };
      };
   };
   parser: { malloyDocument(): unknown };
};

let makeMalloyParser: MakeMalloyParser | null | undefined;

function loadMalloyParser(): MakeMalloyParser | null {
   try {
      // Not a public export: a release that moves it leaves the guard on its whole-text match.
      const entry = createRequire(import.meta.url).resolve(
         "@malloydata/malloy",
      );
      const loaded = createRequire(entry)("./lang/run-malloy-parser") as {
         makeMalloyParser?: MakeMalloyParser;
      };
      return typeof loaded.makeMalloyParser === "function"
         ? loaded.makeMalloyParser
         : null;
   } catch {
      return null;
   }
}

let parserLoader: () => MakeMalloyParser | null = loadMalloyParser;
let warnedNoParser = false;

/** Test seam: swaps the parser loader and forgets the cached factory and the one-time warning. */
export function setMalloyParserLoaderForTest(
   loader?: () => MakeMalloyParser | null,
): void {
   parserLoader = loader ?? loadMalloyParser;
   makeMalloyParser = undefined;
   warnedNoParser = false;
   lexerMissing = false;
   lastLexed = { text: "", prose: [] };
}

/** Malloy's lexer and parser as the translator builds them, with our listeners, so errors are counted and caller text never reaches stderr. */
function malloyParserFactory(): MakeMalloyParser | undefined {
   if (makeMalloyParser === undefined) makeMalloyParser = parserLoader();
   return makeMalloyParser ?? undefined;
}

/** The query path asks about one text at several sites, so the last lex is kept. */
let lastLexed: { text: string; prose: [number, number][] | undefined } = {
   text: "",
   prose: [],
};

/** `no_lexer` is the Malloy parser failing to load, which is ours to fix; `whole_text` is text that would not lex, or a name. */
export type CallerGuardRefusalKind = "lexed" | "whole_text" | "no_lexer";

let lastRefusalKind: CallerGuardRefusalKind = "lexed";
let lexerMissing = false;

/** How the guard's last refusal was reached; read it synchronously after the guard call. */
export function lastCallerGuardRefusalKind(): CallerGuardRefusalKind {
   return lastRefusalKind;
}

function proseRanges(text: string): [number, number][] | undefined {
   if (lastLexed.text !== text)
      lastLexed = { text, prose: lexProseRanges(text) };
   return lastLexed.prose;
}

/**
 * The ranges Malloy reads as prose: the body of a block note, and the payload
 * of a line note, routed exactly to markdown or text. `undefined` (fail closed)
 * on any lexer or parser error, a gap between a block's tokens, an unclosed
 * block, or a token stream not of the expected shape.
 */
function lexProseRanges(text: string): [number, number][] | undefined {
   const factory = malloyParserFactory();
   lexerMissing = !factory;
   if (!factory) {
      // Without the lexer every prose-bearing notebook save and query answers 400, so say why once.
      if (!warnedNoParser) {
         warnedNoParser = true;
         console.warn(
            "Prose-aware caller-annotation guard disabled: @malloydata/malloy ./lang/run-malloy-parser could not be loaded; falling back to whole-text matching.",
         );
      }
      return undefined;
   }
   try {
      const counter = (): ErrorCounter => ({
         errors: 0,
         syntaxError() {
            this.errors++;
         },
      });
      const lexerErrors = counter();
      const parserErrors = counter();
      const { tokenStream, parser } = factory(text, {
         lexerErrorListener: lexerErrors,
         parserErrorListener: parserErrors,
      });
      parser.malloyDocument();
      const vocabulary = tokenStream.tokenSource?.vocabulary;
      if (
         lexerErrors.errors > 0 ||
         parserErrors.errors > 0 ||
         typeof vocabulary?.getSymbolicName !== "function"
      )
         return undefined;
      // ANTLR indexes code points; the regex runs over UTF-16 units.
      const utf16: number[] = [];
      for (let i = 0; i < text.length; ) {
         utf16.push(i);
         i += (text.codePointAt(i) as number) > 0xffff ? 2 : 1;
      }
      utf16.push(text.length);
      const rangeOf = (token: LexedToken): [number, number] | undefined => {
         const from = utf16[token.startIndex];
         const to = utf16[token.stopIndex + 1];
         return from === undefined || to === undefined || to < from
            ? undefined
            : [from, to];
      };
      const prose: [number, number][] = [];
      let block: { prose: boolean; next: number } | undefined;
      for (const token of tokenStream.getTokens()) {
         const name = vocabulary.getSymbolicName(token.type);
         const range = rangeOf(token);
         if (
            name === "BLOCK_ANNOTATION_TEXT" ||
            name === "BLOCK_ANNOTATION_END"
         ) {
            // A lexer that dropped characters inside a block leaves a gap.
            if (!block || !range || token.startIndex !== block.next)
               return undefined;
            if (name === "BLOCK_ANNOTATION_END") {
               block = undefined;
            } else {
               if (block.prose) prose.push(range);
               block.next = token.stopIndex + 1;
            }
            continue;
         }
         if (block) return undefined;
         if (
            name === "BLOCK_ANNOTATION_BEGIN" ||
            name === "DOC_BLOCK_ANNOTATION_BEGIN"
         ) {
            if (!range) return undefined;
            block = {
               prose: PROSE_ROUTES.has(noteRoute(text.slice(...range)) ?? ""),
               next: token.stopIndex + 1,
            };
         } else if (name === "ANNOTATION" || name === "DOC_ANNOTATION") {
            if (!range) return undefined;
            // One token to the end of the line, routed by its prefix, which cannot itself be a hit.
            if (PROSE_ROUTES.has(noteRoute(text.slice(...range)) ?? ""))
               prose.push([range[0] + 1, range[1]]);
         }
      }
      // The lexer reads an unclosed block to the end of the text without complaint.
      return block ? undefined : prose;
   } catch {
      return undefined;
   }
}

/**
 * Whether {@link assertNoCallerAuthorizeAnnotation} would reject this text.
 *
 * Exported so callers deferring to that rejection use the SAME predicate: a
 * narrower near-copy lets a missed spelling through another path.
 *
 * `precedingText` is whatever the compiler reads before `callerText` in the
 * same document, so the caller's text is lexed in the state that text leaves.
 */
export function hasCallerAuthorizeAnnotation(
   callerText: string,
   precedingText = "",
): boolean {
   if (!AUTHORIZE_ANNOTATION_ANYWHERE.test(callerText)) return false;
   const text = precedingText + callerText;
   const bodies = proseRanges(text);
   lastRefusalKind =
      bodies !== undefined ? "lexed" : lexerMissing ? "no_lexer" : "whole_text";
   if (bodies === undefined) return true;
   const sticky = new RegExp(AUTHORIZE_TAG_LIKE, "iuy");
   const callerStart = precedingText.length;
   // A hit never spans a newline, so one reaching the caller's text starts on its first line at the earliest.
   const from = text.lastIndexOf("\n", callerStart - 1) + 1;
   // Bodies and `#` positions both ascend, so one pointer keeps the scan linear.
   let body = 0;
   // Every `#` is a possible match start, so an overlapping hit cannot hide behind an exempt one.
   for (
      let at = text.indexOf("#", from);
      at !== -1;
      at = text.indexOf("#", at + 1)
   ) {
      sticky.lastIndex = at;
      const hit = sticky.exec(text);
      if (!hit) continue;
      const end = at + hit[0].length;
      if (end <= callerStart) continue;
      while (body < bodies.length && bodies[body][1] <= at) body++;
      const [start, stop] = bodies[body] ?? [Infinity, -Infinity];
      if (!(start <= at && end <= stop)) return true;
   }
   return false;
}

/**
 * Reject caller-submitted Malloy text that declares an `authorize` annotation.
 *
 * A source's own gate replaces the base's (`#(authorize) true` above
 * `source: mine is locked_base extend {}` is gated by `true`). That override
 * is only safe while the declaration is the model AUTHOR's, so a caller may
 * not mint one. Restricted mode does not stop it (it covers `##!` flags, not
 * object annotations). A non-gate caller annotation still moves the base's
 * annotations off the struct; that half is closed in the gate walk
 * (`Model.ancestorGateExprs`).
 *
 * A text match is right for a rejection: a false positive is a clear 400, a
 * false negative a bypass. It is a SUPERSET of the parser's spellings (see
 * {@link AUTHORIZE_TAG_LIKE}); whose gate applies stays with the compiled IR.
 *
 * Apply to EVERY caller fragment reaching the compiler: `sourceName` /
 * `queryName` are interpolated verbatim into the `run:` statement.
 *
 * It also fires inside string literals and on `/compile` checks of an
 * author's model — intended, so the message says what to do instead. The one
 * exception is prose located by Malloy's lexer (`#|(markdown)` / `#|(text)`
 * block bodies, `#(markdown)` / `#(text)` line payloads); any lexer or parser
 * error, gap in a block's tokens, or unclosed block falls back to the
 * whole-text match.
 */
export function assertNoCallerAuthorizeAnnotation(
   callerText: string,
   precedingText = "",
): void {
   if (hasCallerAuthorizeAnnotation(callerText, precedingText))
      refuseCallerAuthorizeAnnotation();
}

/** The whole-text form, for a caller NAME that is interpolated mid-line and never lexed on its own. */
export function assertNoAuthorizeTagLike(callerText: string): void {
   if (!AUTHORIZE_ANNOTATION_ANYWHERE.test(callerText)) return;
   lastRefusalKind = "whole_text";
   refuseCallerAuthorizeAnnotation();
}

function refuseCallerAuthorizeAnnotation(): never {
   // A caller-input rejection, so 400 — not ModelCompilationError's 424, which
   // reads as "the package is broken".
   throw new BadRequestError(
      "An `authorize` annotation is not permitted in caller-submitted Malloy " +
         "text. Access gates are declared by the model author on the source; a " +
         "request cannot introduce, replace, or relax one. To validate a gate " +
         "you are authoring, save it to the package's model file and reload the " +
         "package — model load validates every `#(authorize)` annotation it " +
         "declares.",
   );
}

/**
 * Whether ANY of `texts` is an authorize-routed note, well-formed body or not.
 *
 * Does not parse the body: it DETECTS the tag in positions nothing reads it
 * from (see {@link MisplacedAuthorizeAnnotation}).
 *
 * Routed per note, exactly as {@link parseAuthorizeAnnotation} routes — never
 * the document-wide caller scan, which would fail a load over a note that
 * merely MENTIONS the tag (`##(description) "see the #(authorize) tag"`).
 * Matching the parser keeps detection and enforcement in step both ways.
 */
export function containsAuthorizeAnnotationTag(texts: string[]): boolean {
   return texts.some((text) => authorizeNoteContent(text) !== undefined);
}

/**
 * The near-miss spellings among `texts`: notes that read as an attempt at an
 * `authorize` gate but that Malloy does NOT route to `authorize`, so no code
 * path would ever enforce them.
 *
 * The fail-OPEN this prevents: `# (authorize) "$ROLE = 'admin'"` is route
 * `''` (Malloy's MOTLY/render namespace, like `# bar_chart`), so the author
 * reads the source as locked while it serves every row. Such spellings are
 * REFUSED, never honoured: honouring would claim meaning in a namespace Malloy
 * reserves and silently start filtering packages that served every row.
 *
 * Keyed on Malloy's ROUTE, so it cannot refuse another app's namespace:
 *
 *  - `""` (MOTLY) — a near miss only if the payload is a bracketed route word
 *    ({@link motlyAuthorizePayloadPattern}); render tags pass through.
 *  - `undefined` (malformed prefix) — a near miss if the text was reaching for
 *    the route ({@link malformedAuthorizeAttemptPattern}).
 *  - any other NAME — somebody else's route (`#(authorize-v2)`,
 *    `#(authorized)`), left alone, EXCEPT the case, hyphenation and
 *    word-order variants {@link nearMissRouteNames} lists.
 *
 * `route` is REQUIRED so a caller cannot sweep one spelling believing it swept
 * all; {@link collectAuthorizeNearMissesAllRoutes} sweeps all.
 */
export function collectAuthorizeNearMisses(
   texts: Iterable<string>,
   route: string,
): string[] {
   const motlyPattern = motlyAuthorizePayloadPattern(route);
   const malformedPattern = malformedAuthorizeAttemptPattern(route);
   const nearMissNames = new Set(
      nearMissRouteNames(route).map((name) => name.toLowerCase()),
   );
   const found: string[] = [];
   for (const text of texts) {
      const trimmed = text.trimStart();
      const noted = noteRoute(trimmed);
      if (noted === route) continue; // a real gate
      const nearMiss =
         noted === undefined
            ? malformedPattern.test(trimmed)
            : noted === ""
              ? motlyPattern.test(notePayload(trimmed))
              : nearMissNames.has(noted.toLowerCase());
      if (!nearMiss) continue;
      // The first line only — the part that decides routing — so the gate
      // expression stays out of the load error.
      found.push(trimmed.split(/[\r\n]/, 1)[0]);
   }
   return found;
}

/**
 * {@link collectAuthorizeNearMisses} over every
 * {@link RECOGNIZED_AUTHORIZE_SPELLINGS} entry — the sweep load-time callers
 * want.
 */
export function collectAuthorizeNearMissesAllRoutes(
   texts: Iterable<string>,
): string[] {
   const materialized = [...texts];
   return RECOGNIZED_AUTHORIZE_SPELLINGS.flatMap((route) =>
      collectAuthorizeNearMisses(materialized, route),
   );
}

/**
 * Which tag a near-miss spelling was reaching for (the routes have different
 * body rules, so a wrong suggestion earns a second refusal).
 *
 * Decided by the LONGEST matching spelling, not route order: the bare word
 * `authorize` is a substring of `row_authorize`, and route order would send a
 * `#(rowauthorize)` author to the lock.
 */
function guessedNearMissRoute(text: string): string {
   const lower = text.toLowerCase();
   let best = ACCESS_FILTER_ROUTE;
   let bestLength = 0;
   for (const route of RECOGNIZED_AUTHORIZE_SPELLINGS) {
      for (const name of nearMissRouteNames(route)) {
         if (lower.includes(name) && name.length > bestLength) {
            best = route;
            bestLength = name.length;
         }
      }
   }
   return best;
}

/**
 * Refuse a model load carrying any {@link collectAuthorizeNearMisses} spelling.
 *
 * Names every finding at once, so one reload fixes them all.
 */
export function assertNoAuthorizeNearMisses(found: readonly string[]): void {
   if (found.length === 0) return;
   const unique = [...new Set(found)];
   throw new ModelCompilationError({
      message:
         `These annotations are never enforced:\n${unique
            .map(
               (t) => `  - \`${t}\` (meant \`#(${guessedNearMissRoute(t)})\`?)`,
            )
            .join("\n")}\n` +
         `Malloy routes an annotation by its prefix, and only ` +
         `\`#(authorize)\` and \`#(access_filter)\` reach an access-control ` +
         `route (in their \`##\` and ` +
         `block-form \`#|(...)\` spellings too). A space after the \`#\`, spaces ` +
         `inside the brackets, a mis-hyphenated or transposed word, or anything ` +
         `trailing the closing bracket makes it a plain tag Malloy hands to ` +
         `something else. Write the tag named above, unquoted, on its own line ` +
         `directly above the \`source:\` statement you mean to protect (the ` +
         `quoted string form is retired). This is refused rather than ` +
         `interpreted: guessing at the intent would let publisher start ` +
         `enforcing a filter on a package that has been serving every row.`,
   });
}

/**
 * A gate annotation where no authorize code path reads it — so it protects
 * nothing while the model loads and serves as if it did not exist (the
 * fail-OPEN {@link assertNoMisplacedAuthorizeAnnotations} catches).
 *
 *  - `"query"` — on a top-level `query:`. The entry-point walk resolves a run
 *    target through the query's `structRef` (its SOURCE) and never reads the
 *    `NamedQueryDef`'s own annotations. `name` is the query.
 *  - `"field"` — on a `dimension:`/`measure:`/`join_one:`/`view:` line INSIDE
 *    a `source:` block, which lands on the FIELD's annotations. `name` is the
 *    source, `fieldName` the field. EXCLUDES an unannotated join of a gated
 *    source: Malloy copies that source's note onto the join field BY
 *    REFERENCE, which `extractSourcesFromModelDef` recognizes by object
 *    identity or (diamond imports) by parse position.
 *  - `"file"` — a `##(authorize)` on the model itself; nothing reads a
 *    model's own notes for authorize, so it is always a misplacement.
 */
export type MisplacedAuthorizeAnnotation =
   | { kind: "query"; name: string; route: string }
   | { kind: "field"; name: string; fieldName: string; route: string }
   | { kind: "file"; route: string };

/** Human-readable position for a {@link MisplacedAuthorizeAnnotation}. Quotes
 *  the route as AUTHORED, so the author is told to move the tag they actually
 *  wrote. */
function describeMisplacedAuthorizeAnnotation(
   f: MisplacedAuthorizeAnnotation,
): string {
   if (f.kind === "file") return `at the file level (\`##(${f.route})\`)`;
   const tag = `\`#(${f.route})\``;
   if (f.kind === "query") return `on query "${f.name}" (${tag})`;
   return `on field "${f.fieldName}" of source "${f.name}" (${tag})`;
}

/**
 * Refuse a model load that carries a `#(authorize)` annotation in a position
 * nothing enforces — see {@link MisplacedAuthorizeAnnotation}.
 *
 * Throws `ModelCompilationError` naming every finding at once. Shared by
 * `Model.create` and the package-load worker.
 */
export function assertNoMisplacedAuthorizeAnnotations(
   found: readonly MisplacedAuthorizeAnnotation[],
): void {
   if (found.length === 0) return;
   const positions = found
      .map((f) => `  - ${describeMisplacedAuthorizeAnnotation(f)}`)
      .join("\n");
   throw new ModelCompilationError({
      message:
         `An authorize annotation is never enforced at:\n${positions}\n` +
         `A gate only applies where model load looks for one — a \`source:\`'s ` +
         `own annotation, or one it inherits from an \`extend\`/query-source ` +
         `base. A file-level annotation (\`##(authorize)\` / ` +
         `\`##(access_filter)\`) is deprecated and no longer enforced ` +
         `anywhere, so it always lands here: declare the same tag on each ` +
         `\`source:\` it was meant to protect instead. Every other ` +
         `position above should move — with the same tag named beside it — ` +
         `to the \`source:\` statement it is meant ` +
         `to protect.`,
   });
}

/**
 * source name → effective authorize GROUPS (its own, else inherited).
 *
 * A group is one declaring source's own notes, ANDed. When an entry point's
 * gate comes from MORE THAN ONE declaring source (a `query_source` base plus
 * the composite member it resolved to — `effectiveAncestorGateExprs`), each
 * source keeps its OWN group: the fold result is the same (everything ANDs),
 * but flattening would misattribute which source declared which term, and
 * `validateAuthorizeProbes` validates each group independently.
 *
 * The wire shape (`sources[].authorize`) is a flat `string[]`
 * (`extractSourcesFromModelDef` flattens); this map is internal-only.
 *
 * Each group carries its ROUTE, which decides enforcement and must never be
 * collapsed: `"authorize"` is a lock (403 before any graft), `"access_filter"`
 * a grafted `where:`. Grafting a lock as a `where:` would answer a refused
 * caller with a fabricated 200. Own-wins-over-ancestor is also scoped per
 * route.
 */
export type AuthorizeMapGroup = { route: string; exprs: string[] };
export type AuthorizeMap = Map<string, AuthorizeMapGroup[]>;

/**
 * source name → route → the source's OWN-level gate note objects for that
 * route. Backs both `source_extraction.ts`'s presence-based
 * `authorizeOwnNotes` and its attribution-narrowed
 * `attributedAuthorizeOwnNotes` (see that module). Keyed per route so a
 * source can be "own" on one route and "inherited" on the other.
 */
export type AuthorizeOwnNotesMap = Map<string, Map<string, AnnotationNote[]>>;

const GIVEN_REF_PATTERN = /\$([A-Za-z_][A-Za-z0-9_]*)/g;
// A `$NAME` inside a string literal is text, not a given reference
// (`$ROLE = 'the $BOSS role'` references only ROLE), so literals in EITHER
// quote are stripped before scanning. Missing one quote style would report a
// phantom given, and `filterGivensToModelSurface` would then drop a caller
// value instead of failing closed on an unknown given.
const SINGLE_QUOTE_ESCAPE = /\\(['\\])/g;
const DOUBLE_QUOTE_ESCAPE = /\\(["\\])/g;
const STRING_LITERAL_PATTERN = /'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"/g;

/**
 * Given names an authorize expression references (`$NAME` tokens), deduped,
 * in first-seen order. Used by `Model.computeAuthorizeReferencedGivenNames`
 * to find which givens an entry's `exprs` reference.
 */
export function referencedGivenNames(expr: string): string[] {
   const scanned = expr.replace(STRING_LITERAL_PATTERN, "''");
   const names: string[] = [];
   const seen = new Set<string>();
   for (const match of scanned.matchAll(GIVEN_REF_PATTERN)) {
      const name = match[1];
      if (!seen.has(name)) {
         seen.add(name);
         names.push(name);
      }
   }
   return names;
}

// ---------------------------------------------------------------------------
// Row-level gate grammar (the positive allowlist)
// ---------------------------------------------------------------------------

/**
 * Why a row-level gate was refused or warned about. Also the metric label.
 *
 *  - `unreachable_given` (request time, `resolveGateShape`): a given absent
 *    from the model's given surface.
 *  - `entry_point_unexpressible` (load, non-fatal): a valid gate INHERITED by
 *    a derived entry point whose field space no longer carries the field
 *    (renamed/excluded/projected away); that entry point denies every
 *    request.
 *  - `given_usage_unresolvable` (request time): the `refSummary` expansion
 *    (`expandRefSummaryGivenIds`) hit an unresolvable field reference, so the
 *    given SET could not be determined.
 *  - `unclassifiable_condition` (request time): the lifted condition has no
 *    usable `.e` — an unexpected IR shape.
 */
export type RowLevelGateRejectionCause =
   | "unreachable_given"
   | "entry_point_unexpressible"
   // Non-fatal load warnings W1/W2 (`validateSourceLineGateGivenUsage`).
   | "source_line_gate_no_given_reference"
   | "source_line_gate_negated_membership"
   // Fatal at load: the retired quoted-string form.
   | "legacy_string_gate"
   | "given_usage_unresolvable"
   | "unclassifiable_condition";

/**
 * Builds a tuple that must cover EVERY member of `U`. The intersection with
 * `never` on an incomplete list is what makes `tsc` fail at the call site rather
 * than letting the omission through — the argument stops being assignable.
 */
const everyMemberOf =
   <U extends string>() =>
   <T extends readonly U[]>(
      ...members: T & ([U] extends [T[number]] ? unknown : never)
   ): T =>
      members;

/**
 * Every {@link RowLevelGateRejectionCause}: the one list the metric
 * description and the docs read. A union member missing here fails the build.
 */
export const ROW_LEVEL_GATE_REJECTION_CAUSES =
   everyMemberOf<RowLevelGateRejectionCause>()(
      "unreachable_given",
      "entry_point_unexpressible",
      "source_line_gate_no_given_reference",
      "source_line_gate_negated_membership",
      "legacy_string_gate",
      "given_usage_unresolvable",
      "unclassifiable_condition",
   );

/**
 * The result of resolving one row-level gate against the model's given
 * surface: either it is a valid row filter (`givenNames` — the givens the
 * gate compares, used to check every one is on the model's given surface and,
 * at `/compile`, that the caller engaged with each one), or there was nowhere
 * to graft it / it referenced a given off the surface (`rejected`).
 */
export type RowLevelGateClassification =
   | {
        shape: "row_level";
        givenNames: string[];
        /** Given id → name, for the lock's own evaluation of this condition. */
        givenNamesById: ReadonlyMap<string, string>;
     }
   | { shape: "rejected"; cause: RowLevelGateRejectionCause; detail: string };

/**
 * A compiled `FilterCondition` as this module reads it. Duck-typed: Malloy does
 * not export the expression node types from its root, and a deep-path import is
 * a liability in the package-load worker bundle.
 */
export interface CompiledGateCondition {
   /** The gate as the author wrote it, carried through by the compiler. */
   code?: string;
   e?: unknown;
   isSourceFilter?: boolean;
   /** The IR's reference summary (`gate_dimension.ts`'s
    *  `ExpandableRefSummary`), read by callers for G4/W1/W2. Typed loosely to
    *  keep this module free of that import. */
   refSummary?: unknown;
}

/** Minimal materializer surface needed to compile (not run) the probe. */
interface AuthorizeProbeCompiler {
   loadQuery(query: string): { getPreparedQuery(): Promise<unknown> };
}

/**
 * Backtick-quote a Malloy identifier for safe interpolation into a `run:`
 * query string. Escapes backslashes and backticks (in that order) so a name
 * that needs Malloy quoting (hyphen, space, reserved word, leading digit) or
 * contains an embedded backtick cannot break out of the quotes. Mirrors
 * Malloy's unexported `identifierCode`/`escapeIdentifier`.
 */
export function quoteMalloyIdentifier(name: string): string {
   return "`" + name.replace(/\\/g, "\\\\").replace(/`/g, "\\`") + "`";
}

/**
 * Build the ROW-LEVEL probe query text: apply a source's effective authorize
 * expressions as a source-level `where:` directly on `sourceName`, so the
 * gate's field references resolve against THAT entry point's own field space
 * — renames, `except:`/`accept:` drops, and projections included. Must stay
 * the same shape as `gate_classification.ts`'s `liftGateCondition` probe:
 * both read the compiled `FilterCondition` back from
 * `_query.structRef.filterList`.
 */
export function buildRowLevelProbe(
   graftTarget: string,
   filterText: string,
): string {
   return `run: ${quoteMalloyIdentifier(graftTarget)} extend { where: ${filterText} } -> { select: __authorize_probe is 1; limit: 1 }`;
}

/**
 * Read the compiled `FilterCondition` for a {@link buildRowLevelProbe} back out
 * of the prepared query, proving it is the one the probe just asked for.
 *
 * The LAST entry of `structRef.filterList` is where the probe's `where:` lands,
 * but neither property that makes that safe to trust is assumed:
 *
 *  - `code` must equal `filterText`. Otherwise a source's OWN `where:` (or a
 *    Malloy ordering change) hands back the AUTHOR's condition; at request
 *    time `assertGateLanded` would then "prove" the graft against it and the
 *    query would run UNGATED.
 *  - `isSourceFilter` must be true, i.e. the probe still lands where the
 *    design depends on it landing.
 *
 * Shared by the load-time ({@link liftRowLevelCondition}) and request-time
 * (`liftGateCondition`) lifts so the checks cannot drift apart. `label` is the
 * error subject; `T` is the caller's condition type.
 */
export function liftProbeFilterCondition<T extends CompiledGateCondition>(
   prepared: { _query?: { structRef?: { filterList?: T[] } } },
   label: string,
   filterText: string,
): T {
   const filterList = prepared._query?.structRef?.filterList;
   if (!Array.isArray(filterList) || filterList.length === 0) {
      throw new Error(`${label} carries no filter condition`);
   }
   const lifted = filterList[filterList.length - 1];
   if (lifted.code !== filterText) {
      throw new Error(
         `${label} carries the wrong condition — expected "${filterText}", got "${lifted.code ?? ""}"`,
      );
   }
   if (!lifted.isSourceFilter) {
      throw new Error(
         `${label} carries a condition that is not a source filter`,
      );
   }
   return lifted;
}

/**
 * The ONE spelling of a gate entry's whole expression list as a single Malloy
 * boolean. Shared with `resolveGateShape` so the probed text and the grafted
 * text cannot drift — the lifts compare them by string.
 */
export function gateFilterText(exprs: readonly string[]): string {
   return exprs.map((e) => `(${e})`).join(" and ");
}

/**
 * Compile {@link buildRowLevelProbe} and lift its condition via
 * {@link liftProbeFilterCondition}. Throws if the probe fails to compile (an
 * unreachable given, or a field this entry point renamed, excluded or
 * projected away) or the lift cannot be trusted.
 */
async function liftRowLevelCondition(
   compiler: AuthorizeProbeCompiler,
   sourceName: string,
   exprs: string[],
): Promise<CompiledGateCondition> {
   const filterText = gateFilterText(exprs);
   const prepared = (await compiler
      .loadQuery(buildRowLevelProbe(sourceName, filterText))
      .getPreparedQuery()) as {
      _query?: { structRef?: { filterList?: CompiledGateCondition[] } };
   };
   return liftProbeFilterCondition(
      prepared,
      `row-level probe for "${sourceName}"`,
      filterText,
   );
}

/**
 * Load-time validation: compile {@link buildRowLevelProbe} for every entry
 * point in `options.authorizeMap`, so an unknown given or an unresolvable
 * field fails at load instead of first request.
 *
 * Every probe that compiles is handed to `onOwnRowLevelConditionCompiled` for
 * G4/W1/W2 — at EVERY entry point, not just the declaring one, because the
 * probe is recompiled against THIS entry point's given surface (the one
 * `resolveGateShape` grafts against), which can differ two import hops out.
 *
 * A compile failure on a route this source has NO own notes for is an
 * inherited gate it cannot express: it is reported
 * (`onRowLevelGateRejected("entry_point_unexpressible")`,
 * `onRowLevelGateUnexpressible`) without failing the load — safe because
 * `resolveGateShape` denies every request at that entry point. With own
 * notes it throws `ModelCompilationError` naming the source. Shared by
 * `Model.create` and the package-load worker; telemetry and logging come in
 * through callbacks to keep this module bundle-light.
 */
export async function validateAuthorizeProbes(
   compiler: AuthorizeProbeCompiler,
   options: {
      authorizeMap?: AuthorizeMap;
      /**
       * Per-route own notes (from `extractSourcesFromModelDef`); empty for a
       * route the struct only inherits (always, for a `query_source`).
       * Decides own-vs-inherited per group.
       */
      authorizeOwnNotes?: AuthorizeOwnNotesMap;
      onRowLevelGateRejected?: (cause: RowLevelGateRejectionCause) => void;
      /**
       * Called instead of throwing when a gate this entry point INHERITS
       * fails to resolve here — see the doc above. `detail` is the
       * underlying compile failure (Malloy already names the unresolved
       * field/join in it, e.g. `'org_id' is not defined`), for a caller
       * with a logger to report against.
       */
      onRowLevelGateUnexpressible?: (
         sourceName: string,
         detail: string,
      ) => void;
      /**
       * Called with the lifted condition for EVERY group that compiles, own
       * or inherited — despite the name, NOT gated on `authorizeOwnNotes`.
       * The caller runs `validateSourceLineGateGivenUsage` (G4/W1/W2) on it.
       *
       * Must not be gated on attribution: an inheriting entry point
       * recompiles the gate text, so a same-named given its model
       * re-declares WITH a default resolves to that declaration, and
       * attribution cannot reach a declaring source two or more import hops
       * out. Gating would skip G4 exactly there.
       */
      onOwnRowLevelConditionCompiled?: (
         sourceName: string,
         condition: CompiledGateCondition,
         /** The route the gate was collected under, so messages name the
          *  annotation the author actually wrote. */
         route: string,
      ) => void;
   },
): Promise<void> {
   const ownNotesOf =
      options.authorizeOwnNotes ??
      new Map<string, Map<string, AnnotationNote[]>>();

   // Each group is validated INDEPENDENTLY so a rejection names the source
   // that actually declared the term (see `AuthorizeMap`).
   for (const [sourceName, groups] of options.authorizeMap ?? []) {
      for (const { route, exprs } of groups) {
         if (exprs.length === 0) continue;
         let condition: CompiledGateCondition;
         try {
            condition = await liftRowLevelCondition(
               compiler,
               sourceName,
               exprs,
            );
         } catch (err) {
            const detail = err instanceof Error ? err.message : String(err);
            // Own-vs-inherited is decided PER ROUTE.
            const ownNotes = ownNotesOf.get(sourceName)?.get(route) ?? [];
            if (ownNotes.length === 0) {
               options.onRowLevelGateRejected?.("entry_point_unexpressible");
               options.onRowLevelGateUnexpressible?.(sourceName, detail);
               continue;
            }
            throw new ModelCompilationError({
               message:
                  `Invalid #(${route}) annotation on source "${sourceName}" ` +
                  `[${exprs.join(" and ")}]: ${detail}`,
            });
         }
         // Deliberately OUTSIDE the try: a G4/W1/W2 refusal of a gate that
         // DID compile must abort the load regardless of who wrote it (this
         // entry point serves the rows), never fall into the own-vs-inherited
         // warn branch meant for compile failures.
         options.onOwnRowLevelConditionCompiled?.(sourceName, condition, route);
      }
   }
}

/**
 * A double-quoted string literal that consumes an ENTIRE (trimmed) body, start
 * to end — escaped characters (`\"`, `\\`) included. Anchored on both ends
 * deliberately: see {@link isLegacyQuotedPayload}'s doc for why "opens with a
 * quote" is the wrong test.
 */
const WHOLE_BODY_QUOTED_STRING = /^"(?:\\.|[^"\\])*"$/;

/**
 * The same test for a SINGLE-quoted payload (`#(authorize) 'org_id = 999'`):
 * Malloy accepts either quote, so both spellings get the same paste-ready
 * rewrite.
 */
const WHOLE_BODY_SINGLE_QUOTED_STRING = /^'(?:\\.|[^'\\])*'$/;

/**
 * Whether an authorize note's (trimmed) payload is the legacy QUOTED-STRING
 * form (`#(authorize) "<expr>"`) rather than the current unquoted natural-Malloy
 * expression form (`#(authorize) <expr>`) — decided by whether the ENTIRE
 * payload is one quoted string literal, in EITHER quote, not merely whether it
 * opens with a quote. Malloy accepts a quoted string literal as a normal
 * sub-expression (`where: ("admin" = $ROLE)` compiles), so a legal unquoted
 * gate can legitimately START with one (`#(authorize) "admin" = $ROLE`) — a
 * test that only looked at the first character would misidentify it as
 * legacy. Decides only what {@link findLegacyStringGates} reports, not how a
 * payload is parsed.
 */
export function isLegacyQuotedPayload(payload: string): boolean {
   const trimmed = payload.trim();
   return (
      WHOLE_BODY_QUOTED_STRING.test(trimmed) ||
      WHOLE_BODY_SINGLE_QUOTED_STRING.test(trimmed)
   );
}

/**
 * The expression a legacy quoted-string gate MEANT: the payload's string
 * literal decoded to its contents, for {@link assertNoLegacyStringGate}'s
 * paste-ready rewrite (the verbatim payload would reproduce the refused text).
 * Only called on a payload {@link isLegacyQuotedPayload} accepted.
 *
 * Decodes only the quote and backslash escapes; any other backslash sequence
 * passes through as authored rather than being silently mangled.
 */
function unquoteLegacyGatePayload(payload: string): string {
   const trimmed = payload.trim();
   const inner = trimmed.slice(1, -1);
   // Decode only the quote character this payload actually used, so a literal
   // `\'` inside a double-quoted gate (and vice versa) is left as authored.
   return trimmed[0] === "'"
      ? inner.replace(SINGLE_QUOTE_ESCAPE, "$1")
      : inner.replace(DOUBLE_QUOTE_ESCAPE, "$1");
}

/**
 * Parse a single annotation string into its authorize expression.
 *
 * Returns the route and expression for a gate note, `null` for any other
 * note, and throws on an empty body (which load turns into a model error).
 *
 * The body is returned VERBATIM, legacy quoted-string form included: compiled,
 * that form is a string literal rather than a boolean, so the probe fails to
 * lift it and the gate is rejected by construction, with no special case to
 * keep in sync with {@link assertNoLegacyStringGate}.
 *
 * Source- vs file-level is decided by WHERE the note sits, not the `#`/`##`
 * count. Classification is Malloy's ({@link noteRoute}); the body comes from
 * the note's CONTENT, already dedented for the block form.
 */
export interface ParsedAuthorizeAnnotation {
   route: string;
   expr: string;
}

export function parseAuthorizeAnnotation(
   annotation: string,
): ParsedAuthorizeAnnotation | null {
   const parsed = authorizeNoteContent(annotation);
   if (parsed === undefined) return null;
   const trimmed = parsed.content.trim();
   if (trimmed.length === 0) {
      throw new Error("authorize annotation has an empty expression body");
   }
   return { route: parsed.route, expr: trimmed };
}

/**
 * Extract authorize expressions from a list of annotation strings, preserving
 * declaration order. Non-authorize annotations are ignored; there is no dedup —
 * every authorize annotation is an independent term of the gate's conjunction
 * (AND semantics), so repeats are kept. Propagates the throw from a malformed
 * authorize annotation. For one route, use
 * {@link collectAuthorizeExprsForRoute}.
 */
export function collectAuthorizeExprs(
   annotations: string[],
): ParsedAuthorizeAnnotation[] {
   const exprs: ParsedAuthorizeAnnotation[] = [];
   for (const annotation of annotations) {
      const parsed = parseAuthorizeAnnotation(annotation);
      if (parsed !== null) {
         exprs.push(parsed);
      }
   }
   return exprs;
}

/**
 * {@link collectAuthorizeExprs}, narrowed to ONE route's expressions.
 *
 * Parses ALL routes before filtering: a malformed note on EITHER route must
 * fail this call, so a broken note on one route cannot leave the other
 * route's read looking clean. Callers fail closed on the throw
 * (`gateExprsForOwnAnnotations` synthesizes the deny sentinel).
 */
export function collectAuthorizeExprsForRoute(
   annotations: string[],
   route: string,
): string[] {
   return collectAuthorizeExprs(annotations)
      .filter((e) => e.route === route)
      .map((e) => e.expr);
}

/** A source carrying the legacy quoted-STRING form of a gate. `gates` holds
 *  each such OWN (not inherited) annotation's body as authored, quotes and
 *  all, in declaration order, with the route to write the rewrite under. */
export interface LegacyStringGateFinding {
   sourceName: string;
   gates: { route: string; expr: string }[];
}

/**
 * Every top-level source whose OWN gate notes (on either route) include the
 * quoted-string form. Reads only own-level notes, so a source merely
 * inheriting one is not flagged — the declaring base is, once.
 */
export function findLegacyStringGates(
   authorizeOwnNotes: ReadonlyMap<
      string,
      ReadonlyMap<string, AnnotationNote[]>
   >,
): LegacyStringGateFinding[] {
   const found: LegacyStringGateFinding[] = [];
   for (const [sourceName, notesByRoute] of authorizeOwnNotes) {
      const notes = [...notesByRoute.values()].flat();
      const legacyNotes = notes.filter((note) => {
         const content = authorizeNoteContent(note.text);
         return content !== undefined && isLegacyQuotedPayload(content.content);
      });
      if (legacyNotes.length === 0) continue;
      const gates = legacyNotes.flatMap((note) =>
         collectAuthorizeExprs([note.text]),
      );
      if (gates.length > 0) {
         found.push({ sourceName, gates });
      }
   }
   return found;
}

/**
 * Refuse a model load carrying any {@link findLegacyStringGates} finding.
 *
 * The message emits a paste-ready unquoted rewrite per finding, under the
 * finding's OWN route (the routes have different body rules). Callers record
 * `legacy_string_gate` once per finding before calling this.
 *
 * The rewrite puts the annotation on its own line above `source:`: Malloy
 * reads everything after the tag on the same line as annotation text, so a
 * one-line `#(authorize) ... dimension: ...` declares nothing.
 */
export function assertNoLegacyStringGate(
   found: readonly LegacyStringGateFinding[],
): void {
   if (found.length === 0) return;
   const rewrites = found
      .flatMap(({ sourceName, gates }) =>
         gates.map(
            ({ route, expr }) =>
               `  - source "${sourceName}": replace \`#(${route}) ${expr}\`` +
               ` with\n      #(${route}) ${unquoteLegacyGatePayload(expr)}`,
         ),
      )
      .join("\n");
   throw new ModelCompilationError({
      message:
         `The string form of an authorize gate (a Malloy-quoted expression on ` +
         `the \`source:\` line) is no longer accepted. Replace it with the ` +
         `unquoted expression, carried by the same annotation on its own line ` +
         `directly above the \`source:\` line:\n${rewrites}\n` +
         `Test that the rewrite gates the same rows and check the row count ` +
         `matches what the string form served.`,
   });
}
