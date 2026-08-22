// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(authorize)` annotation parsing.
 *
 * Annotation format:
 *   #(authorize)  <malloy-bool-expr>   — source-level, on a single source
 *
 * The body is an UNQUOTED, natural Malloy boolean expression that references
 * declared givens (`$NAME`), e.g. `#(authorize) $ROLE = 'analyst'`. It is taken
 * verbatim off the note — no tokenizing, no unwrapping — and handed to Malloy's
 * own compiler to parse in the target source's real field scope
 * ({@link ../service/authorize}'s `buildRowLevelProbe`). A body that still opens
 * with a double quote is the LEGACY string form and is refused; see
 * {@link assertNoLegacyStringGate}.
 *
 * A file-level `##(authorize)` — the same tag written above a model rather
 * than a source — is deprecated: it is a load error, refused by
 * {@link assertNoMisplacedAuthorizeAnnotations} with a `"file"`-kind
 * {@link MisplacedAuthorizeAnnotation}, naming the remedy (declare
 * `#(authorize)` on each `source:` it should protect) rather than silently
 * applying model-wide the way it used to. The `##` spelling is still
 * RECOGNIZED — by {@link parseAuthorizeAnnotation} and
 * {@link assertNoCallerAuthorizeAnnotation} alike — so an author who writes it
 * gets a clear refusal instead of a silently-ignored annotation.
 *
 * **What counts as the tag is Malloy's answer, not a regex of ours.** A note is
 * a gate iff Malloy routes it to `authorize` ({@link noteRoute}), which
 * admits the block form `#|(authorize)` and the other bracket pairs
 * (`#[authorize]`, `#<authorize>`, `#{authorize}`) and excludes near misses like
 * `# (authorize)` (route `''`, Malloy's reserved MOTLY namespace) and
 * `#( authorize )` (malformed prefix). Publisher must not decide any of that for
 * itself: honouring a spelling Malloy routes elsewhere cements a divergence from
 * the compiler, and missing one Malloy DOES route here serves every row from a
 * source its author locked. The near misses are refused outright at load
 * ({@link collectAuthorizeNearMisses}) rather than either honoured or ignored.
 *
 * This module parses, collects, and (at load time) validates authorize
 * annotations. It does not ENFORCE one: a gate is a row filter grafted onto
 * the entry point by `Model.authorizeAndBindRunnable`, and every probe this
 * module builds is COMPILED, never RUN — the package-load worker's
 * `ProxyConnection.runSQL` deliberately throws (`package_load_worker.ts`).
 * Kept light so it bundles cleanly into the package-load worker: its only
 * non-type imports are `../errors` and `./annotations` (which the worker
 * already bundles via `source_extraction.ts`).
 */

import { payloadOf, routeOf } from "@malloydata/malloy";
import { BadRequestError, ModelCompilationError } from "../errors";
import { type AnnotationNote } from "./annotations";

/** The annotation route Malloy assigns an `authorize` gate. */
const AUTHORIZE_ROUTE = "authorize";

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
 * which is safe. One that accepts LESS is a forged-gate bypass. The two cannot
 * share a definition, because {@link assertNoCallerAuthorizeAnnotation} reads raw
 * PRE-COMPILE text where no route exists yet — a regex is all there is there,
 * while the parser gets to ask {@link noteRoute}.
 *
 * It covers every route-`authorize` spelling by construction: sigil `##?`, an
 * optional block `|`, then `authorize` in any of Malloy's four bracket pairs.
 * Case-insensitive, so a caller cannot mint `#(AUTHORIZE)` past it either.
 *
 * `[ \t]`, never `\s`: the unanchored form scans a whole submitted document, and
 * `\s` matches a newline, so `"# \n(authorize) rules apply"` would be a spurious
 * 400 on ordinary prose.
 *
 * The lookahead requires the name to END at `authorize` — a closing bracket,
 * whitespace, or end of input. Without it this also fired on `#(authorized)` and
 * on the whole `#(authorize-v2)` / `#(authorize.audit)` family, which are other
 * apps' deliberate routes, 400-ing a caller query that merely mentions one.
 */
const AUTHORIZE_TAG_LIKE = String.raw`##?\|?[ \t]*[([{<]?[ \t]*authorize(?=[)\]}>]|[ \t]|$)`;
const AUTHORIZE_ANNOTATION_ANYWHERE = new RegExp(AUTHORIZE_TAG_LIKE, "iu");

/**
 * A MOTLY (route `""`) payload that is an author reaching for a gate: optional
 * space/tab, an opening bracket, `authorize`, a closing bracket. The bracket is
 * REQUIRED, which is what keeps every ordinary render tag out — `# bar_chart`,
 * `# currency`, `# size=lg` have no bracket at that position, and a MOTLY tag
 * genuinely named `authorize` without brackets is left alone rather than guessed
 * at.
 */
const MOTLY_AUTHORIZE_PAYLOAD = /^[ \t]*[([{<][ \t]*authorize[ \t]*[)\]}>]/iu;

/**
 * A malformed-prefix note (route `undefined`) that was reaching for `authorize`
 * — `#( authorize )`, `#(authorize )`, `#(authorize)X`, `#authorize`.
 *
 * Anchored text, and safe to use here precisely BECAUSE the route is already
 * `undefined`: Malloy has refused to give this note a namespace at all, so
 * matching text cannot steal a route that belongs to somebody else.
 */
const MALFORMED_AUTHORIZE_ATTEMPT = /^##?\|?[ \t]*[([{<]?[ \t]*authorize/iu;

/**
 * The gate payload on ONE annotation note, or `undefined` if the note is not an
 * `authorize` gate. Malloy's own routing decides — see {@link noteRoute}. A
 * malformed prefix routes to `undefined`, so it is never a gate here.
 */
function authorizeNoteContent(text: string): string | undefined {
   return noteRoute(text) === AUTHORIZE_ROUTE ? notePayload(text) : undefined;
}

/**
 * Reject caller-submitted Malloy text that declares an `authorize` annotation.
 *
 * A source's own `#(authorize)` replaces the base's when it derives one
 * (`source: mine is locked_base extend {}` carrying `#(authorize) "true"` is
 * gated by "true"). That override is the locked-base + curated-extension idiom,
 * and it is only safe while the declaration is the model AUTHOR's — so a caller
 * may not mint one. Restricted mode does not stop it: its construct rejections
 * cover `##!` compiler-flag annotations, not object annotations.
 *
 * This covers only the forged-gate half. A caller annotation that is NOT an
 * authorize gate still moves the base's annotations off the struct (any
 * annotation does), which is closed in the gate walk itself — see
 * `Model.ancestorGateExprs`.
 *
 * Text-matching is the right tool for a rejection and the wrong one for
 * resolution: a false positive is a clear 400, a false negative is a bypass.
 * Nothing here decides *whose* gate applies — that stays with the compiled IR.
 * It is deliberately a SUPERSET of the parser's spellings, not a mirror of them
 * — see {@link AUTHORIZE_TAG_LIKE} for why that asymmetry is the safe direction
 * and why one shared definition cannot serve both. In particular it must cover
 * the block form and every bracket pair Malloy routes to `authorize`: the
 * classification is the compiler's, so a caller could otherwise mint a gate in a
 * spelling this rejecter had never heard of.
 *
 * Apply to EVERY caller-supplied fragment that reaches the compiler, not just
 * the obvious query body: `sourceName`/`queryName` are interpolated verbatim
 * into the `run:` statement the query path builds, so either one carries an
 * annotation into the compiled text just as effectively.
 *
 * Because it is a byte match over untrusted text, it also fires on an annotation
 * the compiler would never read as a gate — inside a string literal, or in a
 * model an author is compile-checking through `/compile`. That is the intended
 * direction, so the message has to tell an author what to do instead.
 */
export function assertNoCallerAuthorizeAnnotation(callerText: string): void {
   if (!AUTHORIZE_ANNOTATION_ANYWHERE.test(callerText)) return;
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
 * Unlike {@link collectAuthorizeExprs}, this does not parse the body or throw
 * on a malformed one — it exists to DETECT the tag in a position nothing ever
 * reads it FROM at all (see {@link MisplacedAuthorizeAnnotation}), where the
 * fact worth catching is that the author wrote the tag, not whether its body
 * happens to parse.
 *
 * Routed per note, exactly as {@link parseAuthorizeAnnotation} routes, because
 * each `text` here is ONE annotation note rather than a document. The
 * document-wide scan belongs to {@link assertNoCallerAuthorizeAnnotation}, whose
 * input is a whole caller-submitted model where the tag may sit on any line.
 * Using that form here would fail the WHOLE package load over a note that merely
 * MENTIONS the tag — a `##(description) "see the #(authorize) tag"`, or a doc
 * comment quoting it — naming as a gate an annotation the author never wrote.
 * Matching the parser is what keeps the two honest in both directions: a
 * spelling only the detector recognizes refuses a package over text the parser
 * would never have enforced, and one only the parser recognizes lets a real
 * misplaced gate through. This is also why the identity sets built from it
 * (`source_extraction.ts`'s `gatedSourceOwnAuthorizeNotes`, `authorizeOwnNotes`)
 * stay in step with what actually gets enforced.
 */
export function containsAuthorizeAnnotationTag(texts: string[]): boolean {
   return texts.some((text) => authorizeNoteContent(text) !== undefined);
}

/**
 * The near-miss spellings among `texts`: notes that read as an attempt at an
 * `authorize` gate but that Malloy does NOT route to `authorize`, so no code
 * path would ever enforce them.
 *
 * This is the fail-OPEN that motivates the whole route-based classification.
 * `# (authorize) "$ROLE = 'admin'"` is route `''` — Malloy's reserved
 * MOTLY/render namespace, the same route as `# bar_chart` — so it is a plain
 * display tag as far as the compiler is concerned. The author reads their source
 * as locked; it serves every row and the package loads clean.
 *
 * Two responses were available and only one is safe. Teaching publisher to
 * HONOUR the spelling would assign application meaning inside a namespace Malloy
 * reserves, and would silently start enforcing a filter on packages that served
 * every row yesterday — no author action, no error. So the spelling is REFUSED
 * instead: it blesses nothing, it cannot start enforcing anything, and the author
 * gets told what to type.
 *
 * The rule is keyed on Malloy's ROUTE, not on a text superset, so it cannot
 * refuse another app's deliberate namespace. Each of the three route outcomes
 * gets its own answer:
 *
 *  - `""` (MOTLY) — a near miss only if the payload is a bracketed `authorize`
 *    ({@link MOTLY_AUTHORIZE_PAYLOAD}). `# bar_chart`, `# currency` and every
 *    other render tag pass straight through.
 *  - `undefined` (malformed prefix) — a near miss if the text was reaching for
 *    `authorize` ({@link MALFORMED_AUTHORIZE_ATTEMPT}). Malloy has already
 *    refused this note a namespace, so a text test here cannot take one from
 *    anybody.
 *  - any other NAME — somebody else's route, left completely alone. This is
 *    what a text superset got wrong: `#(authorize-v2)`, `#(authorize.audit)`,
 *    `#(authorize/v2)` and `#(authorized)` are all valid distinct routes, and
 *    refusing them failed the whole model load with advice aimed at someone
 *    else. The ONE exception is a case-variant of our own name (`#(AUTHORIZE)`,
 *    `#(Authorize)`): Malloy routes those elsewhere, so honouring them is not an
 *    option, but leaving them silent is the exact fail-open this function
 *    exists for — an author reads the source as locked and every row serves. So
 *    they are refused too, which blesses nothing.
 */
export function collectAuthorizeNearMisses(texts: Iterable<string>): string[] {
   const found: string[] = [];
   for (const text of texts) {
      const trimmed = text.trimStart();
      const route = noteRoute(trimmed);
      if (route === AUTHORIZE_ROUTE) continue; // a real gate
      const nearMiss =
         route === undefined
            ? MALFORMED_AUTHORIZE_ATTEMPT.test(trimmed)
            : route === ""
              ? MOTLY_AUTHORIZE_PAYLOAD.test(notePayload(trimmed))
              : route.toLowerCase() === AUTHORIZE_ROUTE;
      if (!nearMiss) continue;
      // The first line only — the part that decides routing. Reporting the whole
      // note would put the author's gate expression into a load error, and the
      // expression is not what is wrong with it.
      found.push(trimmed.split(/[\r\n]/, 1)[0]);
   }
   return found;
}

/**
 * Refuse a model load carrying any {@link collectAuthorizeNearMisses} spelling.
 *
 * Names every finding at once, like {@link assertNoMisplacedAuthorizeAnnotations},
 * so an author fixing three of them reloads once rather than three times.
 */
export function assertNoAuthorizeNearMisses(found: readonly string[]): void {
   if (found.length === 0) return;
   const unique = [...new Set(found)];
   throw new ModelCompilationError({
      message:
         `These annotations are not \`authorize\` gates and nothing enforces ` +
         `them:\n${unique.map((t) => `  - \`${t}\``).join("\n")}\n` +
         `Malloy routes an annotation by its prefix, and only ` +
         `\`#(authorize)\` (or \`##(authorize)\`, or the block form ` +
         `\`#|(authorize)\`) reaches the authorize route — a space after the ` +
         `\`#\`, spaces inside the brackets, or anything trailing the closing ` +
         `bracket makes it a plain tag Malloy hands to something else. Write ` +
         `\`#(authorize) "<expression>"\` on the \`source:\` statement you mean ` +
         `to protect. This is refused rather than interpreted: guessing at the ` +
         `intent would let publisher start enforcing a filter on a package that ` +
         `has been serving every row.`,
   });
}

/**
 * A `#(authorize)` annotation Malloy attached somewhere that no authorize
 * code path ever reads it FROM — so it protects nothing, and the model loads
 * and serves as if it did not exist. This is the fail-OPEN case
 * {@link assertNoMisplacedAuthorizeAnnotations} exists to catch: unlike a
 * malformed gate (which is invalid IN ITSELF, wherever it sits) or a valid
 * gate one derived entry point can't express (which still gates every OTHER
 * entry point), an annotation in one of these positions is not a gate at all.
 *
 *  - `"query"` — a top-level `query: q is X -> {...}` statement. Malloy
 *    attaches the annotation to the `NamedQueryDef` itself
 *    (`extractQueriesFromModelDef`'s `annotations`), but `Model`'s entry-point
 *    walk resolves a run target through the compiled query's `structRef` —
 *    the SOURCE it derives from — and never reads the `NamedQueryDef`'s own
 *    annotations at all. `name` is the query's own name.
 *  - `"field"` — a `dimension:`/`measure:`/`join_one:`/`view:` line INSIDE a
 *    `source:` block. `extractSourcesFromModelDef` reads the SOURCE's own
 *    `struct.annotations`; an annotation written one line lower lands on that
 *    FIELD's own `annotations` instead, which nothing walks for authorize
 *    purposes. `name` is the source, `fieldName` the field. EXCLUDES an
 *    unannotated `join_one:`/`join_many:` of a gated source: Malloy embeds
 *    the joined source as a nested `StructDef` on the join field and copies
 *    that source's own annotation note object onto the join field's own
 *    annotations BY REFERENCE whenever the join line adds none of its own —
 *    textually indistinguishable from a misplaced annotation, but not one;
 *    see `extractSourcesFromModelDef`'s note-identity check.
 *  - `"file"` — a `##(authorize)` annotation on the model itself rather than
 *    on a `source:`. File-level `##(authorize)` is deprecated: no code path
 *    reads a model's own notes for authorize purposes any more, so this is
 *    always a misplacement, never a "some entry point can express it"
 *    situation — there is no entry point that ever expressed it.
 */
export type MisplacedAuthorizeAnnotation =
   | { kind: "query"; name: string }
   | { kind: "field"; name: string; fieldName: string }
   | { kind: "file" };

/** Human-readable position for a {@link MisplacedAuthorizeAnnotation}, as
 *  {@link assertNoMisplacedAuthorizeAnnotations} names it in its refusal. */
function describeMisplacedAuthorizeAnnotation(
   f: MisplacedAuthorizeAnnotation,
): string {
   if (f.kind === "query") return `on query "${f.name}"`;
   if (f.kind === "file") return "at the file level (`##(authorize)`)";
   return `on field "${f.fieldName}" of source "${f.name}"`;
}

/**
 * Refuse a model load that carries a `#(authorize)` annotation in a position
 * nothing enforces — see {@link MisplacedAuthorizeAnnotation}. Such a position
 * was already silently unenforced before row-level gates existed; this turns
 * it into a load failure instead of a gate that silently protects nothing.
 *
 * Throws `ModelCompilationError` naming every finding — not just the first —
 * so an author with several misplaced annotations sees all of them at once
 * rather than reloading once per fix. Shared by `Model.create` and the
 * package-load worker, called alongside `validateAuthorizeProbes`.
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
         `An \`#(authorize)\` annotation is never enforced at:\n${positions}\n` +
         `A gate only applies where model load looks for one — a \`source:\`'s ` +
         `own annotation, or one it inherits from an \`extend\`/query-source ` +
         `base. File-level \`##(authorize)\` is deprecated and no longer ` +
         `enforced anywhere, so it always lands here: declare \`#(authorize)\` ` +
         `on each \`source:\` it was meant to protect instead. Every other ` +
         `position above should move to the \`source:\` statement it is meant ` +
         `to protect.`,
   });
}

/**
 * source name → effective authorize GROUPS (its own, else inherited).
 *
 * A group is one declaring source's own OR disjunction — unchanged from
 * before groups existed. The outer array is what's new: when an entry point's
 * effective gate is assembled from MORE THAN ONE declaring source (a
 * `query_source` base plus, separately, the composite member Malloy resolved
 * that base to — see `gate_registry_walk.ts`'s `effectiveAncestorGateExprs`),
 * each source's own list is kept as its OWN group rather than concatenated
 * into one. Concatenating them was the bug: this file's own rule (see
 * `Model`'s doc on `collectAuthorizeEntryPointGates`) is AND across gates
 * from different sources, OR only within one source's own list — flattening
 * two sources' lists into one `string[]` silently turned that AND into an OR,
 * because `validateAuthorizeProbes`/`gateFilterText` treat any one `string[]`
 * as a single source's own disjunction. `validateAuthorizeProbes` classifies
 * and validates each group independently for exactly this reason.
 *
 * A source that declares its own `#(authorize)` (possibly more than one,
 * genuinely OR'd) still gets exactly ONE group — grouping only ever splits
 * apart gates that came from DIFFERENT declaring sources, never a single
 * source's own annotations.
 *
 * The WIRE shape (`sources[].authorize`) stays a flat `string[]` for
 * introspection — see `extractSourcesFromModelDef`, which flattens the groups
 * before setting it. This map is internal-only.
 */
export type AuthorizeMap = Map<string, string[][]>;

const GIVEN_REF_PATTERN = /\$([A-Za-z_][A-Za-z0-9_]*)/g;
// Malloy string literals are single-quoted; a `$NAME` inside one is literal
// text, not a given reference. Strip literals (honoring `\'` escapes) before
// scanning, so e.g. `$ROLE = 'the $BOSS role'` references only ROLE.
const STRING_LITERAL_PATTERN = /'(?:\\.|[^'\\])*'/g;

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
 * Why a row-level gate was refused at publish. Also the metric label.
 *
 * `unreachable_given` is a request-time re-check
 * ({@link ../service/gate_classification}'s `resolveGateShape`): the gate
 * accepted a given absent from the model's own given surface.
 * `entry_point_unexpressible` is different in kind: the gate is a valid,
 * declared gate, but at one entry point that did not itself declare it — a
 * derived source (an `extend` that renamed/excluded/projected away the
 * field, or a `query_source` projection) inheriting a source's gate — the
 * field it reads did not resolve at all, so there was nothing to probe
 * successfully. That case does not fail the load — see
 * `validateAuthorizeProbes`'s doc for how "own vs inherited" is decided.
 * `given_usage_unresolvable` is a request-time classification failure
 * distinct from `unreachable_given`: the lifted condition's own
 * `refSummary`-following expansion (`./gate_dimension`'s
 * `expandRefSummaryGivenIds`) hit a field reference it could not resolve on
 * the graft target's struct — the given SET could not be fully determined,
 * not that a determined given fell off the model's surface.
 * `unclassifiable_condition` is the lifted condition carrying no usable
 * expression at all (missing/malformed `.e`) — an IR shape this classifier
 * has never seen, not a string-form finding, so it gets its own cause rather
 * than reusing `legacy_string_gate`'s metric label.
 */
export type RowLevelGateRejectionCause =
   | "unreachable_given"
   | "entry_point_unexpressible"
   // Non-fatal — `./gate_dimension`'s `validateSourceLineGateGivenUsage`
   // W1/W2, warned rather than refused; the model still loads. See that
   // module's doc.
   | "source_line_gate_no_given_reference"
   | "source_line_gate_negated_membership"
   // Task 3 (the mechanical migration off the string form) produces this —
   // reserved here now so the cause union and this list never drift apart.
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
 * Every {@link RowLevelGateRejectionCause}, as the one list the metric
 * description and the docs both read.
 *
 * Retyping the list into prose is what let it drift — an operator alerting on
 * a cause they had never been told about had nowhere to look it up. Adding a
 * member to the union without adding it here now fails the build.
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
   | { shape: "row_level"; givenNames: string[] }
   | { shape: "rejected"; cause: RowLevelGateRejectionCause; detail: string };

/**
 * A compiled `FilterCondition` as this module reads it. Duck-typed rather than
 * imported: Malloy does not re-export the expression node types from its
 * package root (the same situation as `given.ts`'s `MalloyGiven`), and this
 * module is bundled into the package-load worker, where a type-only import of a
 * deep path is a liability.
 */
export interface CompiledGateCondition {
   /** The gate as the author wrote it, carried through by the compiler. */
   code?: string;
   e?: unknown;
   isSourceFilter?: boolean;
   /** The IR's own reference-tracking summary for this condition —
    *  `./gate_dimension`'s `ExpandableRefSummary` shape. Read by
    *  `validateAuthorizeProbes`'s caller to run G4/W1/W2 for the SOURCE-LINE
    *  form (`validateSourceLineGateGivenUsage`); this module stays free of
    *  that import (see the module doc's bundling note), so the field is
    *  typed loosely here and cast at the read site. */
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
 * contains an embedded backtick cannot break out of the quotes. `Model`
 * (`service/model.ts`) has its own independently-justified copy for its own
 * callers (mirroring Malloy's internal `identifierCode`/`escapeIdentifier`,
 * which is not exported); this module uses it to quote the graft target in
 * {@link buildRowLevelProbe} below.
 */
export function quoteMalloyIdentifier(name: string): string {
   return "`" + name.replace(/\\/g, "\\\\").replace(/`/g, "\\`") + "`";
}

/**
 * Build the ROW-LEVEL probe query text: apply a source's effective authorize
 * expressions as a source-level `where:` directly on `sourceName`, so the
 * gate's field references resolve against THAT entry point's own field space
 * — renames, `except:`/`accept:` drops, and projections included. `__authorize_probe`
 * is a reserved, deliberately obscure select name, matching `./gate_classification`'s
 * `liftGateCondition`'s identical probe shape (kept in lockstep with it: both
 * need the SAME shape to read back the compiled `FilterCondition` from
 * `_query.structRef.filterList`).
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
 *  - `code` must equal `filterText`. Without it, a source carrying its OWN
 *    `where:` — or a Malloy ordering change — hands back the AUTHOR's condition
 *    instead of the gate's, which `validateAuthorizeProbes` would then treat
 *    as a successful probe of a filter that is not the gate at all. At
 *    request time it is worse: `assertGateLanded` "proves" the graft landed
 *    by matching this same entry's `code`, so the proof would pass against
 *    the author's filter and the query would run UNGATED.
 *  - `isSourceFilter` must be true, ruling out a condition that is not a
 *    source-level filter at all — i.e. this probe shape no longer landing where
 *    the whole design depends on it landing.
 *
 * Genuinely shared by both lifts — load-time validation's
 * {@link liftRowLevelCondition} and request-time `./gate_classification`'s
 * `liftGateCondition` — rather than spelled out twice. They read the same IR
 * path and ran the same three checks independently, differing only in the
 * error label, and a check that drifted out of one of them fails in the
 * direction above. `label` is the subject phrase for those errors; `T` is the
 * caller's own condition type, so the service path keeps Malloy's
 * `FilterCondition` and the worker-bundled path keeps the duck type.
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
 * boolean: `exprs.map(e => "(" + e + ")").join(" or ")`. Shared with
 * `./gate_classification`'s `resolveGateShape`'s `filterText` so the text this
 * module probes and the text the request path grafts can never drift apart —
 * they are compared against each other, by string, in
 * {@link liftRowLevelCondition} and `./gate_classification`'s
 * `liftGateCondition`.
 */
export function gateFilterText(exprs: readonly string[]): string {
   return exprs.map((e) => `(${e})`).join(" or ");
}

/**
 * Compile {@link buildRowLevelProbe} and lift the condition Malloy built for the
 * `where:` this probe just added, via {@link liftProbeFilterCondition} — which is
 * where the three checks that make the lift trustworthy live, shared with
 * `./gate_classification`'s `liftGateCondition` rather than duplicated beside it.
 *
 * Throws if the probe fails to compile (an unreachable given, or — the case this
 * exists to catch — a field the gate references that this entry point renamed,
 * excluded, or projected away) or if the lift itself cannot be trusted.
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
 * Translation-time validation: compile {@link buildRowLevelProbe} for every
 * entry point in `options.authorizeMap` — the gate applied as a source-level
 * `where:` on that entry point itself — surfacing an unknown given or a
 * source-field reference this entry point cannot resolve at model-load
 * instead of first request. A successful compile at the DECLARING entry
 * point still needs G4/W1/W2 against the lifted condition's own given usage
 * — see `onOwnRowLevelConditionCompiled` below — since a compile succeeding
 * says nothing about whether the gate references a defaulted given or
 * negates a membership test.
 *
 * A compile failure is either a genuinely broken gate, or an entry point that
 * INHERITED a gate it cannot express — a derived source (an `extend` that
 * renamed/excluded/projected away the field, or a `query_source` projection)
 * whose own field space no longer carries what the gate reads. "Own vs
 * inherited" is answered directly, per entry, by `options.authorizeOwnNotes`
 * (from `extractSourcesFromModelDef`): empty means this struct carries no
 * `#(authorize)`-tagged note of its own at all (a `query_source` struct has
 * no `annotations` by construction, so it can only ever be inheriting) —
 * there is no authoring mistake to blame on THIS entry point, so it escapes
 * via {@link recordRowLevelGateRejected} (`onRowLevelGateRejected`,
 * `entry_point_unexpressible`) and a human-readable warning
 * (`onRowLevelGateUnexpressible`; this module stays free of both the
 * telemetry stack and a logger, see the module doc) WITHOUT failing the
 * load: `Model.resolveGateShape` denies every request against this one entry
 * point regardless (see its doc), so leaving the load to continue does not
 * leak anything. A non-empty own-notes entry has nowhere else to point the
 * blame, so it throws.
 *
 * Throws `ModelCompilationError` naming the source on the first invalid
 * annotation. Shared by `Model.create` and the package-load worker so both
 * compile paths validate identically.
 */
export async function validateAuthorizeProbes(
   compiler: AuthorizeProbeCompiler,
   options: {
      authorizeMap?: AuthorizeMap;
      /**
       * source name → the source's OWN-level `#(authorize)`-tagged note
       * objects (from `extractSourcesFromModelDef`) — empty when the struct
       * carries no annotation of its own at all (e.g. a `query_source`,
       * which has no `annotations` by construction). This is what "own vs
       * inherited" is decided on below.
       */
      authorizeOwnNotes?: Map<string, AnnotationNote[]>;
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
       * Called with the successfully-lifted condition for a group `sourceName`
       * declares OWN (`authorizeOwnNotes` non-empty) — never for a group it
       * only INHERITS. This module stays free of a `./gate_dimension` import
       * (see the module doc's bundling note), so it hands the compiled
       * condition back rather than running G4/W1/W2 itself; the caller (both
       * `Model.create` and the package-load worker already import
       * `./gate_dimension`) runs `validateSourceLineGateGivenUsage` against
       * it. "Own" is the same distinction the throw-vs-warn branch below
       * already draws: only the DECLARING source's own condition is checked —
       * an inheritor gets no second check, since the expression and its
       * referenced givens are identical to what was already checked where it
       * was declared.
       */
      onOwnRowLevelConditionCompiled?: (
         sourceName: string,
         condition: CompiledGateCondition,
      ) => void;
   },
): Promise<void> {
   const ownNotesOf =
      options.authorizeOwnNotes ?? new Map<string, AnnotationNote[]>();

   // One `sourceName` may carry MORE THAN ONE group — see `AuthorizeMap`'s
   // doc. Each group is validated INDEPENDENTLY, exactly as a single-group
   // source always was: that independence is what keeps two groups' AND
   // semantics intact instead of silently flattening them into one OR'd
   // disjunction (the bug this grouping exists to fix).
   for (const [sourceName, groups] of options.authorizeMap ?? []) {
      for (const exprs of groups) {
         if (exprs.length === 0) continue;
         try {
            const condition = await liftRowLevelCondition(
               compiler,
               sourceName,
               exprs,
            );
            const ownNotes = ownNotesOf.get(sourceName) ?? [];
            if (ownNotes.length > 0) {
               options.onOwnRowLevelConditionCompiled?.(sourceName, condition);
            }
         } catch (err) {
            const detail = err instanceof Error ? err.message : String(err);
            const ownNotes = ownNotesOf.get(sourceName) ?? [];
            if (ownNotes.length === 0) {
               options.onRowLevelGateRejected?.("entry_point_unexpressible");
               options.onRowLevelGateUnexpressible?.(sourceName, detail);
               continue;
            }
            throw new ModelCompilationError({
               message:
                  `Invalid #(authorize) annotation on source "${sourceName}" ` +
                  `[${exprs.join(" | ")}]: ${detail}`,
            });
         }
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
 * Whether an authorize note's (trimmed) payload is the legacy QUOTED-STRING
 * form (`#(authorize) "<expr>"`) rather than the current unquoted natural-Malloy
 * expression form (`#(authorize) <expr>`) — decided by whether the ENTIRE
 * payload is one double-quoted string literal, not merely whether it opens
 * with a quote. Malloy accepts a double-quoted string literal as a normal
 * sub-expression (`where: ("admin" = $ROLE)` compiles), so a legal unquoted
 * gate can legitimately START with one (`#(authorize) "admin" = $ROLE`) — a
 * test that only looked at the first character would misidentify it as
 * legacy.
 *
 * This no longer decides how an expression is PARSED — see
 * {@link parseAuthorizeAnnotation}'s doc, which returns every payload
 * verbatim regardless of form. Its only remaining job is deciding what
 * {@link findLegacyStringGates} reports at load time.
 */
function isLegacyQuotedPayload(payload: string): boolean {
   return WHOLE_BODY_QUOTED_STRING.test(payload.trim());
}

/**
 * Parse a single annotation string into its authorize expression.
 *
 * Returns the inner expression for a well-formed `#(authorize)` / `##(authorize)`
 * annotation, `null` if the string is not an authorize annotation at all, and
 * throws if it looks like one but has an empty body. The throw is what later
 * compile-time validation turns into a model-load error.
 *
 * The body is the annotation's expression VERBATIM, unquoted and unmodified,
 * with NO EXCEPTION for the legacy quoted-string form
 * ({@link isLegacyQuotedPayload}) — that form's payload (e.g.
 * `"org_id = 999"`) is returned exactly as authored, quotes and all, and
 * compiled downstream as an ordinary Malloy expression. Compiled that way it
 * is a STRING LITERAL, not a boolean, so `resolveGateShape`'s probe fails to
 * lift it ("Filter expression must have boolean value") and the gate is
 * rejected — the legacy form dies by construction wherever an expression is
 * actually evaluated, rather than by a special case that has to be kept in
 * sync with {@link findLegacyStringGates}'s load-time refusal. See
 * {@link assertNoLegacyStringGate} for that refusal.
 *
 * Whether the annotation is source- or file-level is decided by WHERE the note
 * sits (a struct's `blockNotes` vs the model's own notes), not by the `#`/`##`
 * count, so the one route covers both.
 *
 * Classification is Malloy's — see {@link noteRoute} and this module's
 * header. That is what makes the block form (`#|(authorize)` … `|#`) a gate here
 * as it already is to the compiler, and what keeps `#( authorize )` /
 * `#(authorize)X` from being honoured as gates publisher alone believes in. The
 * body then comes from the note's own CONTENT rather than from slicing a matched
 * prefix off the text, which is what makes the multi-line form work: Malloy has
 * already dedented the block's body and dropped its closer.
 */
export function parseAuthorizeAnnotation(annotation: string): string | null {
   const content = authorizeNoteContent(annotation);
   if (content === undefined) return null;
   const trimmed = content.trim();
   if (trimmed.length === 0) {
      throw new Error("authorize annotation has an empty expression body");
   }
   return trimmed;
}

/**
 * Extract authorize expressions from a list of annotation strings, preserving
 * declaration order. Non-authorize annotations are ignored; there is no dedup —
 * every authorize annotation is an independent gate (OR semantics), so repeats
 * are kept. Propagates the throw from a malformed authorize annotation.
 */
export function collectAuthorizeExprs(annotations: string[]): string[] {
   const exprs: string[] = [];
   for (const annotation of annotations) {
      const expr = parseAuthorizeAnnotation(annotation);
      if (expr !== null) {
         exprs.push(expr);
      }
   }
   return exprs;
}

/** A source found declaring more than one `#(authorize)` block note of its
 *  own (source-line position) — see {@link assertAtMostOneAuthorizeGate}.
 *  `texts` is the raw note text of each, in declaration order. */
export interface MultipleAuthorizeGateFinding {
   sourceName: string;
   texts: string[];
}

/**
 * Every top-level source that carries MORE THAN ONE `#(authorize)`-tagged
 * note of its own (source-line position). A source may declare at most one.
 * The admin-override idiom this might look like it forecloses
 * (`#(authorize) "$ROLE = 'admin'"` OR'd with a second block) is still fully
 * expressible — as a single natural boolean, `#(authorize) $ROLE = 'admin'
 * or org_id in $GROUPS` — since the current form's payload is an ordinary
 * Malloy expression, not a name that can only refer to one field.
 *
 * Reads `authorizeOwnNotes` ({@link ../service/source_extraction.ts}'s
 * companion to `authorizeMap`, already own-level-only, struct-annotation-only)
 * so this only ever counts SOURCE-LINE notes — a field's own annotation
 * note (misplaced, since `#(authorize)` only gates from the `source:` line)
 * never appears here.
 */
export function findMultipleAuthorizeGates(
   authorizeOwnNotes: ReadonlyMap<string, AnnotationNote[]>,
): MultipleAuthorizeGateFinding[] {
   const found: MultipleAuthorizeGateFinding[] = [];
   for (const [sourceName, notes] of authorizeOwnNotes) {
      if (notes.length > 1) {
         found.push({ sourceName, texts: notes.map((note) => note.text) });
      }
   }
   return found;
}

/**
 * Refuse a model load carrying any {@link findMultipleAuthorizeGates}
 * finding, naming every source and every one of its offending annotations at
 * once — same "name everything found" posture as
 * {@link assertNoMisplacedAuthorizeAnnotations}/{@link assertNoAuthorizeNearMisses}.
 */
export function assertAtMostOneAuthorizeGate(
   found: readonly MultipleAuthorizeGateFinding[],
): void {
   if (found.length === 0) return;
   const positions = found
      .map(
         ({ sourceName, texts }) =>
            `  - source "${sourceName}" declares ${texts.length}:\n` +
            texts
               .map((t) => `      \`${t.trim().split(/[\r\n]/, 1)[0]}\``)
               .join("\n"),
      )
      .join("\n");
   throw new ModelCompilationError({
      message:
         `A source may declare at most one \`#(authorize)\` annotation:\n${positions}\n` +
         `Combine multiple conditions into one expression with \`or\` instead of ` +
         `repeating the annotation, e.g. ` +
         "`#(authorize) $ROLE = 'admin' or org_id in $GROUPS`.",
   });
}

/** A source found carrying the legacy STRING form of `#(authorize)` — a
 *  Malloy-quoted expression annotated on the `source:` line itself, now
 *  refused in favor of the dimension form. `exprs` is the parsed body of
 *  each such annotation the source declares OWN (not inherited), in
 *  declaration order — the exact text {@link assertNoLegacyStringGate}
 *  writes back into its rewrite. */
export interface LegacyStringGateFinding {
   sourceName: string;
   exprs: string[];
}

/**
 * Every top-level source that still carries the string form — a
 * Malloy-quoted expression annotated on the `source:` line — among its OWN
 * `#(authorize)` notes. Reads only `authorizeOwnNotes`
 * ({@link ../service/source_extraction.ts}'s companion to `authorizeMap`,
 * already own-level-only), so this cannot flag a source that merely
 * INHERITS a string-form gate from a base — the base itself carries its own
 * annotation and is flagged there instead, keeping the refusal atomic per
 * declaring source rather than firing once per entry point that inherits
 * it.
 *
 * Filters to {@link isLegacyQuotedPayload} notes before parsing — the current
 * unquoted natural-expression form routes to `authorize` exactly the same way
 * and must not be caught by this refusal, only the quoted one.
 */
export function findLegacyStringGates(
   authorizeOwnNotes: ReadonlyMap<string, AnnotationNote[]>,
): LegacyStringGateFinding[] {
   const found: LegacyStringGateFinding[] = [];
   for (const [sourceName, notes] of authorizeOwnNotes) {
      const legacyNotes = notes.filter((note) => {
         const content = authorizeNoteContent(note.text);
         return content !== undefined && isLegacyQuotedPayload(content);
      });
      if (legacyNotes.length === 0) continue;
      const exprs = collectAuthorizeExprs(legacyNotes.map((note) => note.text));
      if (exprs.length > 0) {
         found.push({ sourceName, exprs });
      }
   }
   return found;
}

/**
 * Refuse a model load carrying any {@link findLegacyStringGates} finding.
 *
 * The string form (`#(authorize) "<expr>"` on the `source:` line) is no
 * longer accepted — every gate must be the SOURCE-LINE form, an unquoted
 * Malloy expression carried by an `#(authorize)` annotation on its own line
 * directly above the `source:` line it gates. The message writes the
 * rewrite back to the author rather than pointing at docs: this function
 * already holds the exact expression text, so it emits the exact
 * replacement annotation the author can paste in, one per finding.
 *
 * Named `legacy_string_gate` on {@link RowLevelGateRejectionCause} / the
 * `recordRowLevelGateRejected` metric channel — callers record that cause
 * once per finding before calling this, so an operator can count affected
 * packages across the deployed (uninventoried) corpus during rollout.
 *
 * The emitted rewrite puts `#(authorize)` and the expression it gates on
 * their own line above `source:` — the only legal shape for this form, so
 * there is no one-line trap to fall into here. That trap — Malloy consuming
 * everything after `#(authorize)` on the SAME line as annotation text, so a
 * one-line `#(authorize) internal dimension: authorized is ...` never
 * declares an actual `dimension:` field at all (the whole line became
 * annotation text) and the source silently loads with NO gate — still exists
 * for anyone who writes that shape by hand; there is no `dimension:`-based
 * gate spelling this rewrites to, and `internal dimension: authorized is
 * ...` is not itself an authorize expression, so this class of mistake never
 * reaches this function's rewrite at all. See task-3-fix-brief.md C1.
 */
export function assertNoLegacyStringGate(
   found: readonly LegacyStringGateFinding[],
): void {
   if (found.length === 0) return;
   const rewrites = found
      .flatMap(({ sourceName, exprs }) =>
         exprs.map(
            (expr) => `  - source "${sourceName}":\n      #(authorize) ${expr}`,
         ),
      )
      .join("\n");
   throw new ModelCompilationError({
      message:
         `The string form of \`#(authorize)\` (a Malloy-quoted expression on ` +
         `the \`source:\` line) is no longer accepted. Replace it with the ` +
         `unquoted expression, carried by an \`#(authorize)\` annotation on ` +
         `its own line directly above the \`source:\` line:\n${rewrites}\n` +
         `Test that the rewrite gates the same rows and check the row count ` +
         `matches what the string form served.`,
   });
}
