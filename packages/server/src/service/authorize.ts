/**
 * `#(authorize)` annotation parsing.
 *
 * Annotation format:
 *   #(authorize)  "<malloy-bool-expr>"   — source-level, on a single source
 *
 * The body is a single double-quoted Malloy boolean expression that references
 * declared givens (`$NAME`), e.g. `#(authorize) "$ROLE = 'analyst'"`. The
 * expression itself routinely contains single quotes (Malloy string literals),
 * so we cannot reuse filter.ts's whitespace tokenizer — we unwrap exactly one
 * layer of double quotes and hand the inner expression back untouched.
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
 * annotations, and classifies a compiled gate against the row-level grammar
 * ({@link classifyAuthorizeGate}). It does not ENFORCE one: a gate is a row
 * filter grafted onto the entry point by `Model.authorizeAndBindRunnable`,
 * and the only queries this module runs are load-time probes. Kept light so
 * it bundles cleanly into the
 * package-load worker: its only non-type imports are `../errors` and
 * `./annotations` (which the worker already bundles via `source_extraction.ts`).
 */

import { payloadOf, routeOf, type GivenValue } from "@malloydata/malloy";
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

/**
 * Build the synthetic probe query that evaluates a source's authorize
 * expressions. Each expression becomes a boolean `select` column over a
 * one-row, warehouse-independent DuckDB source (the `"duckdb"` sandbox is
 * registered for every package, so this never touches the model's real
 * warehouse). Compiling this probe validates the expressions against the
 * model's `given:` block (unknown givens and source-field references surface as
 * compile errors); running it evaluates the gate. The reserved dummy column
 * name is deliberately obscure so a real authorize expression is unlikely to
 * collide with it — a bare field reference in an expression is meant to fail.
 *
 * Always compiles against the AMBIENT given namespace of the model it runs
 * in. It used to accept its own `given:` declarations, for a self-contained
 * per-branch probe of a joined source's gate; a gate is enforced as a row
 * filter on the entry point now, so nothing probes a branch that way and the
 * two remaining callers (`runOneRowProbeOrThrow`,
 * `assertNoVacuousDefaultAtom`) both want the ambient namespace.
 */
export function buildAuthorizeProbe(exprs: string[]): string {
   const selects = exprs
      .map((expr, i) => `__auth_${i} is (${expr})`)
      .join("\n      ");
   return `run: duckdb.sql("SELECT 1 AS __authorize_probe_row") -> {
    select:
      ${selects}
    limit: 1
  }`;
}

const GIVEN_REF_PATTERN = /\$([A-Za-z_][A-Za-z0-9_]*)/g;
// Malloy string literals are single-quoted; a `$NAME` inside one is literal
// text, not a given reference. Strip literals (honoring `\'` escapes) before
// scanning, so e.g. `$ROLE = 'the $BOSS role'` references only ROLE — otherwise
// a joined gate's referenced-count is inflated and the full-coverage check
// wrongly denies a correctly-authorized request.
const STRING_LITERAL_PATTERN = /'(?:\\.|[^'\\])*'/g;

/**
 * Given names an authorize expression references (`$NAME` tokens), deduped,
 * in first-seen order. Used to figure out which givens a self-contained
 * probe needs to declare for a given expression.
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
 * The first six come from {@link classifyAuthorizeGate}: the gate's compiled
 * condition IS readable and is not an allowed shape — invalid IN ITSELF,
 * wherever it is probed from. `vacuous_default_atom` is also a property of the
 * gate itself, found by probing rather than by shape — see
 * `assertNoVacuousDefaultAtom`. `entry_point_unexpressible` is different in
 * kind: the gate is a valid, allowed shape, but at one entry point that did
 * not itself declare it — a derived source (an `extend` that
 * renamed/excluded/projected away the field, or a `query_source` projection)
 * inheriting a source's gate — the field it reads did not resolve at
 * all, so there was no condition to classify. That case does not fail the
 * load — see `validateAuthorizeProbes`'s doc, in particular for how it
 * confirms the gate is genuinely inherited rather than an
 * independently-authored one that merely shares text with something else: by
 * the gate's own annotation NOTE OBJECT, not its string.
 */
export type RowLevelGateRejectionCause =
   | "array_given_needs_in"
   | "scalar_given_rejects_in"
   | "field_given_has_default"
   | "unsupported_node"
   | "no_given_reference"
   | "unreachable_given"
   | "vacuous_default_atom"
   | "entry_point_unexpressible";

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
 * Retyping the list into prose is what let it drift: the metric help listed six
 * of these (`field_given_has_default` missing, though it is emitted) and
 * `docs/authorize.md` five (also missing `entry_point_unexpressible`) — so an
 * operator alerting on a cause they had never been told about had nowhere to look
 * it up. Adding a member to the union without adding it here now fails the build.
 */
export const ROW_LEVEL_GATE_REJECTION_CAUSES =
   everyMemberOf<RowLevelGateRejectionCause>()(
      "array_given_needs_in",
      "scalar_given_rejects_in",
      "field_given_has_default",
      "unsupported_node",
      "no_given_reference",
      "unreachable_given",
      "vacuous_default_atom",
      "entry_point_unexpressible",
   );

export type RowLevelGateClassification =
   // `givenNames` is the walker's own record of which givens the gate compared,
   // and it earns its place by driving the `no_given_reference` rejection below.
   // A companion list of the FIELD paths the walk visited is deliberately NOT
   // returned: nothing enforces on field identity — a gate is applied by
   // grafting its whole compiled condition and proving that landed — and both
   // uses such a list invites are foreclosed. Naming a field to the caller is
   // what the error-scrubbing posture forbids, and deciding at load time which
   // entry points a gate's fields must resolve at is already answered by
   // compiling the probe there, which is the authority; a second, weaker
   // classifier beside it is what must not exist.
   //
   // `literalAtoms` is the reconstructed source text of every `<given> <op>
   // <literal>` atom the walk accepted (e.g. `$ROLE != 'admin'` from the
   // admin-override idiom) — self-contained Malloy boolean expressions,
   // independently probeable. A gate is a per-request filter, so any one of
   // these being TRUE under the given's own DECLARATION DEFAULT (the value a
   // caller who supplies nothing gets) makes the whole disjunction it sits in
   // admit every row for that caller — see `validateAuthorizeProbes`'s
   // default-value probe, which is what actually evaluates these.
   | { shape: "row_level"; givenNames: string[]; literalAtoms: string[] }
   | { shape: "rejected"; cause: RowLevelGateRejectionCause; detail: string };

/**
 * The compiled-condition node kinds a row-level gate may be built from, and
 * how to descend each one.
 *
 * A gate is an access-control rule, so the set of shapes it may take should be
 * small enough to read in one screen. Everything absent from this table is
 * refused at publish — a function call, arithmetic, a literal comparison, a
 * `like`. Widening it is a decision someone makes on purpose. A bare boolean
 * literal (`true`/`false`) IS accepted, handled separately below — it is the
 * old whole-source admit/deny, expressed as a constant row filter.
 */
const BOOLEAN_NODES = new Set(["and", "or"]);
/** Wrappers that contribute no semantics of their own; descend through `.e`. */
const TRANSPARENT_NODES = new Set(["()"]);
/** Comparisons legal against a SCALAR given. */
const SCALAR_COMPARISON_NODES = new Set(["=", "!=", ">", ">=", "<", "<="]);

/** Bound on the compiled-condition walk; a real gate is a handful of nodes. */
const MAX_GATE_WALK_DEPTH = 64;

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
   refSummary?: { fieldUsage?: unknown[]; givenUsage?: unknown[] };
   isSourceFilter?: boolean;
}

/**
 * Classify a gate from its COMPILED condition — the `FilterCondition` Malloy
 * produces for `source: … extend { where: <gate> }`.
 *
 * Reading the compiled IR rather than the annotation text is what keeps this
 * exact. Malloy has already resolved which names are fields and which are
 * givens, so the walk below tells a field reference from a given reference
 * with no guessing. A text scan cannot answer it: `$ROLE` and `region` are
 * both bare words. Every gate is enforced as a row filter now, whether or not
 * its condition happens to read a field — a field-less condition (`$ROLE =
 * 'admin'`) is simply constant across every row.
 *
 * It also collapses a distinction the plan expected to police by hand. `org_id
 * ? $GROUPS` and `org_id = $GROUPS` compile to the SAME `=` node, so there is
 * nothing to tell apart: both are a scalar comparison against an array-typed
 * given, which is rejected below on the given's declared TYPE. Type, not
 * spelling, is what decides the operator.
 *
 * `declaredTypes` is this model's own given surface. A gate referencing a given
 * that is not on it is refused rather than guessed: the type is what picks the
 * legal operator, so an unknown type cannot be checked at all. That refusal
 * doubles as the given-reachability check — Malloy does not flatten a `given:`
 * declaration past one import hop, so a gate whose given lives two hops away
 * would otherwise silently bind that given's declaration DEFAULT at request
 * time.
 *
 * `declaredDefaults` is this model's given surface again, but the DECLARED
 * DEFAULT text rather than the type — the value a caller who supplies nothing
 * gets. A `<field> <op> $GIVEN` comparison (a real row-level gate, as opposed
 * to the `<given> <op> <literal>` admin-override atom handled separately) is
 * refused outright when `$GIVEN` carries one: `tenant != $EXCLUDED` with
 * `EXCLUDED` defaulting to `''` compiles to `WHERE tenant != ''`, which admits
 * nearly every row for a caller who supplies nothing, and the same failure
 * mode hits `>`/`>=` against a numeric zero default. This is not about the
 * OPERATOR — `<=`/`>=` against a given with NO default (e.g. a no-read-up
 * `clearance <= $MAXLVL`) is a legitimate gate and stays accepted; a given
 * with no default has no hazard at all, since an unsupplied one fails the
 * request outright ("has no value and no default") rather than silently
 * resolving to anything. See `assertNoVacuousDefaultAtom` for the sibling
 * check on the literal-atom side, which this doesn't overlap with: that one
 * PROBES an atom's truth against its default; this one refuses at the shape
 * level because a field comparison can't be probed the same way (the
 * "default" side is a fixed value, but the FIELD side ranges over every row).
 *
 * Fails CLOSED: an unreadable condition is a rejection, never a pass.
 */
export function classifyAuthorizeGate(
   condition: CompiledGateCondition,
   declaredTypes: Map<string, string>,
   declaredDefaults: Map<string, string>,
): RowLevelGateClassification {
   const givenNames: string[] = [];
   const literalAtoms: string[] = [];
   let rejection: RowLevelGateClassification | undefined;

   const reject = (
      cause: RowLevelGateRejectionCause,
      detail: string,
   ): false => {
      rejection ??= { shape: "rejected", cause, detail };
      return false;
   };

   /** The given a comparison operand names, or null if it is not a given. */
   const givenOperand = (node: unknown): string | null => {
      const n = asNode(node);
      return n && n.node === "given" && typeof n.refName === "string"
         ? n.refName
         : null;
   };

   /**
    * Whether a comparison operand is a bare literal — `numberLiteral`,
    * `stringLiteral`, or the boolean literal nodes, which Malloy's compiler
    * discriminates as the node kinds `"true"` / `"false"` themselves rather
    * than a `literal` value on a shared `booleanLiteral` kind (confirmed
    * against `@malloydata/malloy`'s `BooleanLiteralNode`). A given-vs-literal
    * comparison is the admin-override atom (`$ROLE = 'admin'`); anything else
    * on this side of a given comparison — a field, a function call, a
    * `like` — stays refused.
    */
   const isLiteralOperand = (node: unknown): boolean => {
      const n = asNode(node);
      return (
         !!n &&
         (n.node === "numberLiteral" ||
            n.node === "stringLiteral" ||
            n.node === "true" ||
            n.node === "false")
      );
   };

   /**
    * Render an {@link isLiteralOperand} node back to Malloy source text, so
    * an accepted atom can be re-probed on its own (see `literalAtoms` on
    * {@link RowLevelGateClassification}). `stringLiteral`'s `.literal` is the
    * raw string VALUE (confirmed against `@malloydata/malloy`'s
    * `expr-string.js`, which sets it from the author's unescaped source), not
    * already-quoted Malloy syntax, so it is re-quoted here; `numberLiteral`'s
    * `.literal` is already valid Malloy numeric source text and is emitted
    * verbatim. Returns `null` for anything `isLiteralOperand` did not accept
    * — callers only reach this after that check passes, so `null` here would
    * mean the two functions disagree.
    */
   const literalOperandText = (node: unknown): string | null => {
      const n = asNode(node);
      if (!n) return null;
      if (n.node === "true" || n.node === "false") return n.node;
      if (n.node === "numberLiteral" && typeof n.literal === "string") {
         return n.literal;
      }
      if (n.node === "stringLiteral" && typeof n.literal === "string") {
         return `'${n.literal.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
      }
      return null;
   };

   /** The declared type of a given, or a rejection if it is not reachable. */
   const declaredTypeOf = (name: string): string | null => {
      const declared = declaredTypes.get(name);
      if (declared === undefined) {
         reject(
            "unreachable_given",
            `\`$${name}\` is not on this model's given surface, so the gate ` +
               `would bind its declaration default rather than the caller's ` +
               `value. Declare it here, importing it if it lives elsewhere ` +
               `(\`import { ${name} } from "…"\`)`,
         );
         return null;
      }
      return declared;
   };

   const walk = (node: unknown, depth: number): boolean => {
      if (depth > MAX_GATE_WALK_DEPTH) {
         return reject("unsupported_node", "the gate nests too deeply to read");
      }
      const n = asNode(node);
      if (!n || typeof n.node !== "string") {
         return reject("unsupported_node", "the gate has an unreadable shape");
      }
      const kind = n.node;
      if (BOOLEAN_NODES.has(kind)) {
         const kids = asNode(n.kids);
         if (!kids) {
            return reject("unsupported_node", `\`${kind}\` has no operands`);
         }
         return walk(kids.left, depth + 1) && walk(kids.right, depth + 1);
      }
      if (TRANSPARENT_NODES.has(kind)) {
         return walk(n.e, depth + 1);
      }
      if (kind === "true" || kind === "false") {
         // A bare boolean literal — the old whole-source admit/deny, now
         // expressed as a constant row filter (`where: true` / `where:
         // false`) rather than a separate enforcement mechanism. Recorded as
         // a literal atom purely so `assertNoVacuousDefaultAtom` can skip it
         // by name without a runtime probe — its truth is already static,
         // and neither value is the vacuous-default hazard that check exists
         // to catch.
         literalAtoms.push(kind);
         return true;
      }
      if (kind === "inGiven") {
         // `field in $ARRAY` — the only correct spelling for an array given,
         // and the only one that fails CLOSED on an empty array (`WHERE FALSE`).
         if (n.not === true) {
            return reject(
               "unsupported_node",
               "a negated membership test (`not in`) is not an access rule; " +
                  "write the gate as the set of rows a caller MAY read",
            );
         }
         const given = givenOperand(n.givenRef);
         if (given === null) {
            return reject(
               "unsupported_node",
               "`in` must test membership of a declared given",
            );
         }
         const declared = declaredTypeOf(given);
         if (declared === null) return false;
         if (!isArrayType(declared)) {
            return reject(
               "scalar_given_rejects_in",
               `\`$${given}\` is declared \`${declared}\` (a scalar), so ` +
                  `\`in $${given}\` is not a membership test. Compare it with ` +
                  `\`=\` instead`,
            );
         }
         givenNames.push(given);
         // The membership side is usually a FIELD (`org_id in $GROUPS`, the
         // row-level idiom), but it can also be another GIVEN (`$TENANT in
         // $ALLOWED` — is the caller's tenant one of the values it also
         // supplied). That reads no row field at all, so — same as a `<given>
         // <op> <literal>` atom — it is self-contained and constant for the
         // life of one request; probed the same way by
         // `assertNoVacuousDefaultAtom`.
         const otherGiven = givenOperand(n.e);
         if (otherGiven !== null) {
            // Reachability is checked on THIS operand too, not just the
            // membership side: an unreachable given binds its declaration
            // default at request time instead of the caller's value.
            if (declaredTypeOf(otherGiven) === null) return false;
            givenNames.push(otherGiven);
            literalAtoms.push(`$${otherGiven} in $${given}`);
            return true;
         }
         return walkFieldOperand(n.e);
      }
      if (SCALAR_COMPARISON_NODES.has(kind)) {
         return walkScalarComparison(kind, n.kids, false);
      }
      if (kind === "not") {
         // Negating a membership test (`not in`) is refused above — an empty
         // given then matches every row. Negating a plain scalar comparison
         // carries no such hazard (`not ($ROLE = 'blocked')` is exactly
         // `$ROLE != 'blocked'`), so it is accepted for that one shape only;
         // anything else under a `not` (a compound `and`/`or`, a membership
         // test) is refused rather than reasoned about generically.
         let inner = asNode(n.e);
         while (inner && TRANSPARENT_NODES.has(inner.node as string)) {
            inner = asNode(inner.e);
         }
         if (!inner || !SCALAR_COMPARISON_NODES.has(inner.node as string)) {
            return reject(
               "unsupported_node",
               "`not` may only wrap a single `<field-or-given> <operator> " +
                  "<given-or-literal>` comparison; a negated membership test " +
                  "or compound condition is not an access rule",
            );
         }
         return walkScalarComparison(inner.node as string, inner.kids, true);
      }
      return reject(
         "unsupported_node",
         `\`${kind}\` is not permitted in a gate; a row-level gate is a ` +
            `boolean combination of \`<field> <operator> $GIVEN\` comparisons`,
      );
   };

   /** `<given> <op> <literal-or-field>`, optionally wrapped in `not (...)`. */
   const walkScalarComparison = (
      kind: string,
      kidsNode: unknown,
      negate: boolean,
   ): boolean => {
      const kids = asNode(kidsNode);
      if (!kids) {
         return reject("unsupported_node", `\`${kind}\` has no operands`);
      }
      const left = givenOperand(kids.left);
      const right = givenOperand(kids.right);
      const given = left ?? right;
      if (given === null) {
         return reject(
            "no_given_reference",
            `\`${kind}\` must compare a field against a given; a comparison ` +
               `against a constant is a fixed filter and belongs in the ` +
               `source's own \`where:\``,
         );
      }
      const declared = declaredTypeOf(given);
      if (declared === null) return false;
      if (isArrayType(declared)) {
         // Both `org_id = $GROUPS` and `org_id ? $GROUPS` arrive here: they
         // compile to the same node. Each one compiles clean and then fails
         // in the warehouse (`WHERE "org_id"=ARRAY[7,8]` is a cast error),
         // so it is a broken gate rather than a strict one.
         return reject(
            "array_given_needs_in",
            `\`$${given}\` is declared \`${declared}\` (an array), so ` +
               `comparing it with \`${kind}\` compiles and then fails in the ` +
               `warehouse. Write \`in $${given}\` — it is also the spelling ` +
               `that matches no rows when the array is empty`,
         );
      }
      const otherSide = left === null ? kids.left : kids.right;
      if (isLiteralOperand(otherSide)) {
         // `<given> <op> <literal>` — e.g. the admin-override disjunct
         // `$ROLE = 'admin'`. Constant for the life of one request, so
         // it is an all-rows-or-no-rows term inside the `where:`, exactly
         // like the whole-source boolean this gate would have been before
         // row-level gates existed. Legal as an ATOM inside a row-level
         // gate.
         givenNames.push(given);
         const literalText = literalOperandText(otherSide);
         if (literalText !== null) {
            // Side order matters for a non-commutative operator (`>`,
            // `<`, `>=`, `<=`): reconstruct `$given op literal` when the
            // given was on the left, `literal op $given` when it was on
            // the right, rather than always writing the given first.
            const atom =
               left !== null
                  ? `$${given} ${kind} ${literalText}`
                  : `${literalText} ${kind} $${given}`;
            // `assertNoVacuousDefaultAtom` probes this exact text, so a
            // negation must be reflected here — probing the un-negated atom
            // would check the wrong condition.
            literalAtoms.push(negate ? `not (${atom})` : atom);
         }
         return true;
      }
      // A real row-level comparison — the given's other side is a FIELD,
      // which ranges over every row. Refuse it outright if `$given` carries
      // a declared default: a caller who supplies nothing gets whatever
      // rows that default admits (`> 0`, `!= ''`, … each admit nearly every
      // row), and there is no way to probe a field-vs-given comparison the
      // way `assertNoVacuousDefaultAtom` probes a literal atom, because the
      // FIELD side isn't a fixed value to evaluate against. A given with NO
      // default carries no such hazard — see the function doc — so only a
      // DECLARED default is refused here, not the comparison operator.
      if (declaredDefaults.has(given)) {
         return reject(
            "field_given_has_default",
            `\`${kind}\` compares row field data against \`$${given}\`, ` +
               `which is declared with a default (\`${declaredDefaults.get(given)}\`). ` +
               `A caller who supplies no value for \`$${given}\` gets that ` +
               `default, and the comparison then applies to every row — ` +
               `admitting rows it was meant to exclude. Declare \`$${given}\` ` +
               `with no default so a caller must supply one explicitly`,
         );
      }
      givenNames.push(given);
      return walkFieldOperand(otherSide);
   };

   /** The non-given side of a comparison: a plain field reference, nothing else. */
   const walkFieldOperand = (node: unknown): boolean => {
      const n = asNode(node);
      if (!n || n.node !== "field" || !Array.isArray(n.path)) {
         return reject(
            "unsupported_node",
            `a gate compares a FIELD against a given; \`${
               asNode(node)?.node ?? "that operand"
            }\` is not a field reference`,
         );
      }
      return true;
   };

   let ok: boolean;
   try {
      ok = walk(condition.e, 0);
   } catch {
      // Fail closed: a condition we cannot read is not a gate we can enforce.
      return {
         shape: "rejected",
         cause: "unsupported_node",
         detail: "the gate's compiled shape could not be read",
      };
   }
   if (!ok) {
      return (
         rejection ?? {
            shape: "rejected",
            cause: "unsupported_node",
            detail: "the gate is not an allowed shape",
         }
      );
   }
   // A bare boolean literal (`true`/`false`) reaches here with no given
   // reference at all — that is the accepted constant-predicate idiom (see
   // the `walk` branch above), not the case this guards against. Anything
   // else that walked successfully with no given pushed to `literalAtoms`
   // either — every accepted node either names a given or is a bare literal
   // — so `literalAtoms.length === 0` here means the walk found neither.
   if (givenNames.length === 0 && literalAtoms.length === 0) {
      return {
         shape: "rejected",
         cause: "no_given_reference",
         detail:
            "a row-level gate must compare a field against a given; a gate " +
            "that references none is a fixed filter and belongs in the " +
            "source's own `where:`",
      };
   }
   return { shape: "row_level", givenNames, literalAtoms };
}

/**
 * Whether a given's declared type is an array.
 *
 * The string comes from `ApiGiven.type`, which `malloyGivenToApi` renders from
 * Malloy's type DISCRIMINATOR — so an array given arrives as the bare word
 * `"array"`, not as `"string[]"`. (See the note above `malloyGivenToApi` in
 * `given.ts`: the element type is dropped on the way to the wire.) Matching only
 * a `[]` suffix silently classified every array given as a scalar, which turned
 * `field in $ARRAY` — the one spelling that is correct AND fail-closed on an
 * empty array — into a per-request denial that told the author to use `=`, which
 * is itself rejected.
 *
 * The `[]` suffix is still accepted so a caller holding a source-authored
 * spelling is not misread, but `"array"` is the one production emits.
 */
function isArrayType(declaredType: string): boolean {
   const normalized = declaredType.trim().toLowerCase();
   return normalized === "array" || normalized.endsWith("[]");
}

/** Narrow an unknown IR node to an indexable object, or null. */
function asNode(node: unknown): Record<string, unknown> | null {
   return node !== null && typeof node === "object"
      ? (node as Record<string, unknown>)
      : null;
}

/**
 * Strict, fail-closed truthiness for a probe result cell. DuckDB (the probe's
 * connection) returns a native boolean, but normalize defensively: only a real
 * `true` / SQL `1` / `"true"` grants access. null, undefined, `0`, `false`, a
 * missing cell — anything else — denies.
 */
export function isProbeTrue(cell: unknown): boolean {
   return cell === true || cell === 1 || cell === "true";
}

/** Minimal materializer surface needed to compile (not run) the probe. */
interface AuthorizeProbeCompiler {
   loadQuery(query: string): { getPreparedQuery(): Promise<unknown> };
}

/** Minimal materializer surface needed to run the probe (the runtime gate). */
interface AuthorizeProbeExecutor {
   loadQuery(query: string): {
      run(opts: {
         rowLimit?: number;
         givens?: Record<string, GivenValue>;
      }): Promise<{ data: { value: ReadonlyArray<Record<string, unknown>> } }>;
   };
}

/**
 * Run one probe (already-built query text) and report whether it evaluated
 * true. Any throw (compile error, runtime "no value" for a referenced given,
 * a missing/malformed result) propagates to the caller, which decides how to
 * react — `assertNoVacuousDefaultAtom` treats one specific message as the
 * benign no-default case and every other as a load failure.
 */
async function runProbe(
   executor: AuthorizeProbeExecutor,
   probeText: string,
   givens: Record<string, GivenValue>,
): Promise<boolean> {
   const result = await executor
      .loadQuery(probeText)
      .run({ rowLimit: 1, givens });
   const row = result?.data?.value?.[0];
   return !!(row && isProbeTrue(row.__auth_0));
}

/**
 * Backtick-quote a Malloy identifier for safe interpolation into a `run:`
 * query string. Escapes backslashes and backticks (in that order) so a name
 * that needs Malloy quoting (hyphen, space, reserved word, leading digit) or
 * contains an embedded backtick cannot break out of the quotes. Mirrors
 * `Model`'s private `quoteMalloyIdentifier` in `service/model.ts` — same
 * escaping rules, duplicated here (rather than shared) so this module keeps
 * its light import surface (see the module doc comment: it must stay
 * importable by the package-load worker).
 */
function quoteMalloyIdentifier(name: string): string {
   return "`" + name.replace(/\\/g, "\\\\").replace(/`/g, "\\`") + "`";
}

/**
 * Build the ROW-LEVEL probe query text: apply a source's effective authorize
 * expressions as a source-level `where:` directly on `sourceName`, so the
 * gate's field references resolve against THAT entry point's own field space
 * — renames, `except:`/`accept:` drops, and projections included — rather
 * than the synthetic one-row source `buildAuthorizeProbe` uses, which has no
 * real columns at all. `__authorize_probe` is a reserved, deliberately
 * obscure select name, same convention as `buildAuthorizeProbe`'s
 * `__auth_N` and `./gate_classification`'s `liftGateCondition`'s identical
 * probe shape (kept in lockstep with it: both need the SAME shape to read
 * back the compiled `FilterCondition` from `_query.structRef.filterList`).
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
 *    instead of the gate's. At load time `classifyAuthorizeGate` would then decide
 *    the gate's enforcement shape from a filter that is not the gate (a real
 *    gate reads `row_level`; a plain author filter reads `rejected`,
 *    failing the load of a package whose gate is fine). At request time it is
 *    worse: `assertGateLanded` "proves" the graft landed by matching this same
 *    entry's `code`, so the proof would pass against the author's filter and the
 *    query would run UNGATED.
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
 * Run the one-row `buildAuthorizeProbe`, wrapping a failure into the
 * `ModelCompilationError` shape `validateAuthorizeProbes` has always thrown.
 *
 * The message interpolates the gate's own expression text, which is deliberate
 * and discloses nothing new IN THIS THREAT MODEL: `sources[].authorize` already
 * publishes a source's effective expressions (see `api-doc.yaml`), and the API
 * carries no authentication of its own (`docs/security-posture.md`), so the text
 * is already readable by anyone who can reach this server. Two precisions,
 * because the argument is narrower than "same surface": the interpolated text
 * can name a gate declared on a struct `sources[]` does NOT publish — a
 * derivation base, or a composite member — and these are
 * `ModelCompilationError`s raised on the operator's own package load, not
 * answers to a caller's query. The author-only assumption is NOT what makes it
 * safe; what makes it safe is that the effective expressions are published
 * already.
 * Called by the "no ancestor to blame this on" fallback below, when a
 * row-level probe failed to compile and no other entry point already proved
 * this exact gate object sound — surfacing the plain one-row probe's error
 * is a friendlier diagnostic than the row-level probe's own compile failure.
 */
async function runOneRowProbeOrThrow(
   compiler: AuthorizeProbeCompiler,
   sourceName: string,
   exprs: string[],
): Promise<void> {
   try {
      await compiler.loadQuery(buildAuthorizeProbe(exprs)).getPreparedQuery();
   } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      throw new ModelCompilationError({
         message: `Invalid #(authorize) annotation on source "${sourceName}" [${exprs.join(" | ")}]: ${detail}`,
      });
   }
}

/**
 * Malloy's own wording (`expression_compiler.js`) when a given is referenced
 * with no caller-supplied value and no declared default — the ONE safe
 * outcome for {@link assertNoVacuousDefaultAtom}'s no-givens probe: a given
 * with no default cannot silently resolve to anything at request time
 * either, so there is no vacuous-default hazard to refuse. Matched by
 * substring rather than parsed structurally: this module has no access to
 * Malloy's internal error types, and the message is the only stable surface.
 */
const NO_DEFAULT_GIVEN_PATTERN = /has no value and no default/;

/**
 * Refuse a row-level gate whose accepted literal atom(s) (`literalAtoms` from
 * {@link classifyAuthorizeGate} — a `<given> <op> <literal>` comparison like
 * the admin-override `$ROLE != 'admin'`) evaluate TRUE against the given's
 * own DECLARATION DEFAULT — the value a caller who supplies nothing gets.
 * The admin-override idiom is an OR disjunct specifically so a caller need
 * not name every non-admin role; the flip side is that the atom's truth is
 * then decided by whichever value the given resolves to when the caller
 * supplies nothing, and the documented
 * convention is an EMPTY default. A `!=` (or any comparison the empty/zero
 * default doesn't happen to satisfy) atom is vacuously true there, and an
 * OR'd atom that is always true makes the whole disjunction — the whole row
 * filter — admit every row for that caller. This is the same failure mode a
 * negated membership test (`not in`) was refused for in
 * {@link classifyAuthorizeGate}: an authoring mistake to catch at load time,
 * not a runtime condition to document as a trap.
 *
 * Probes with NO supplied givens (`{}`), so each atom's own given resolves to
 * its declared default exactly as it would for a caller who supplied
 * nothing. A given with NO default at all cannot be vacuous this way — a
 * caller must supply a value or the request itself fails the identical "no
 * value and no default" way — so that one outcome is not a refusal; see
 * {@link NO_DEFAULT_GIVEN_PATTERN}. Any OTHER probe failure (a given the
 * classify walk already proved reachable should not fail its OWN atom's
 * probe) still fails the load — fail closed, matching every other check in
 * this function.
 */
async function assertNoVacuousDefaultAtom(
   executor: AuthorizeProbeExecutor,
   sourceName: string,
   literalAtoms: string[],
): Promise<void> {
   for (const atom of literalAtoms) {
      // A bare boolean literal's truth value is already known statically —
      // no given to probe, so no runtime connection is needed at all (this
      // runs inside the package-load worker, whose `ProxyConnection` cannot
      // `runSQL`). Neither is vacuous: this check exists to catch an atom
      // that is UNINTENTIONALLY always true (a given resolving to its own
      // declared default), not to forbid `#(authorize) "true"` — a deliberate
      // constant admit, the direct replacement for the old whole-source
      // admit/deny this task collapsed into a row predicate.
      if (atom === "false" || atom === "true") continue;
      let vacuous: boolean;
      try {
         vacuous = await runProbe(executor, buildAuthorizeProbe([atom]), {});
      } catch (err) {
         const detail = err instanceof Error ? err.message : String(err);
         if (NO_DEFAULT_GIVEN_PATTERN.test(detail)) continue;
         throw new ModelCompilationError({
            message:
               `Invalid #(authorize) annotation on source "${sourceName}": ` +
               `the atom \`${atom}\` could not be evaluated against its ` +
               `given's declared default (${detail}).`,
         });
      }
      if (vacuous) {
         throw new ModelCompilationError({
            message:
               `Invalid #(authorize) annotation on source "${sourceName}": ` +
               `the atom \`${atom}\` evaluates to TRUE when a caller supplies ` +
               `no givens (its given's own declared default). An OR'd atom ` +
               `that is true by default makes the whole row filter admit ` +
               `every row for that caller. Give the given a default this ` +
               `atom evaluates false against, or declare it with no default ` +
               `so a caller must supply one explicitly.`,
         });
      }
   }
}

/**
 * Translation-time validation. Type mismatches such as `$ROLE = 5` are NOT
 * Malloy compile errors, so they are not caught here — they fail closed at
 * the runtime gate.
 *
 * Validation is shape-aware: it works from `options.authorizeMap` (source
 * name → EFFECTIVE gate GROUPS, inheritance included, from
 * `extractSourcesFromModelDef` — see {@link AuthorizeMap}'s doc for why a
 * source name maps to more than one group) — the full entry-point list, not
 * just the declaring source. Each group is classified and validated on its
 * own, never merged with a sibling group from the same entry point. A gate's
 * field reference can resolve at the source that
 * declares it and still fail at an entry point that renamed, excluded, or
 * projected the field away (`rename:`, `except:`, `accept:`, a `->`
 * projection) — and loading the model successfully is NOT evidence this
 * can't happen; the break
 * otherwise surfaces only when a real query touches it. This covers only
 * entry points in THIS model: `compileMalloyModel` compiles each file
 * independently, so an importing model's own entry points get validated when
 * THAT model loads; anything this pass misses fails closed at request time
 * (`Model.resolveGateShape` denies rather than leaks) instead of leaking.
 *
 * For each entry point in `authorizeMap`:
 *  - First compile {@link buildRowLevelProbe} — the gate applied as a
 *    source-level `where:` on the entry point itself — via
 *    `getPreparedQuery()` (compile-only; unlike `getSQL()` it does not
 *    require a given to have a bound value, so an unbound given is not
 *    confused with a field-resolution failure).
 *  - If it compiles, lift the condition and classify it with
 *    {@link classifyAuthorizeGate}:
 *     - `row_level` — the gate's compiled shape is a valid row filter (whether
 *       or not it happens to read a field), and its field(s), if any, resolved
 *       at this entry point; still checked for a vacuous default atom below,
 *       nothing further beyond that.
 *     - `rejected` — record {@link recordRowLevelGateRejected} (via
 *       `options.onRowLevelGateRejected`, so this module itself never
 *       imports the telemetry stack) and throw, naming the source, the gate
 *       text, and the rejection detail.
 *  - If it does NOT compile, the entry is set aside as PENDING rather than
 *    decided immediately — see the two-pass note below for why: a compile
 *    failure alone cannot tell "genuinely broken gate" apart from "this ONE
 *    derived entry point renamed/excluded/projected away the field(s), and
 *    every other entry point that shares the identical gate text is fine".
 *
 * `sourceName` is deliberately NOT used to make that call: whether
 * `sourceName` is among the sources that DECLARE a gate cannot tell apart a
 * source that wrote its own annotation from one Malloy merely copied it onto.
 * Malloy shares a base's annotation NOTE objects, by reference, onto a
 * derived struct's OWN `annotations` whenever the deriving statement adds no
 * annotation of its own — `extend {}`, `extend { rename: … }`, `extend {
 * except: … }`, `extend { accept: … }` all do this. `W_rename`, `W_except`,
 * and `W_accept` — the exact class of "unexpressible at this one entry
 * point" case this scoping exists FOR — therefore look exactly as
 * "declaring" as the source that actually wrote the annotation, by every
 * signal short of comparing note object identity.
 *
 * Gate TEXT is equally unsafe for the same question ("did this exact gate
 * validate somewhere else"): two sources can independently type the
 * identical gate string (same givens, same convention, no relation to each
 * other) and get TWO SEPARATE note objects — one parsed occurrence each. Text
 * matching cannot tell that apart from `W_rename` genuinely sharing `X`'s
 * note object by reference, so a source whose OWN declaration was broken
 * would escape to a warning purely because an unrelated neighbor happened to
 * type the same string and validated fine. Object identity is exactly the
 * discriminator fix1's join-field scan (`source_extraction.ts`) also relies
 * on for the identical reason: it is shared ONLY when Malloy itself copied
 * the reference, never when two authors independently wrote the same text.
 *
 * So the question this function asks is: **is this failing entry's gate
 * genuinely INHERITED** — either it carries no annotation of its own at all
 * (a `query_source` struct has no `annotations` whatsoever, so it can only
 * ever be inheriting), or its own annotation note object is the SAME
 * object, by reference, as one that validated successfully somewhere else
 * in this model? If yes, some OTHER entry point already proved the gate
 * sound and this one just can't express it — denies at request time instead
 * (`Model.resolveGateShape`). If the failing entry's own note is a DISTINCT
 * object (independently authored, even if textually identical to something
 * else), there is no ancestor to blame it on: the load fails exactly as it
 * always has.
 *
 * `options.authorizeOwnNotes` (from `extractSourcesFromModelDef`) carries
 * only a struct's OWN-level authorize-tagged notes. There is no file-level
 * entry folded in: a `##(authorize)` never reaches this function at all, as
 * `assertNoMisplacedAuthorizeAnnotations` refuses the load for one before
 * this runs.
 *
 * TWO PASSES over `authorizeMap`, because that answer isn't known until every
 * entry has been tried, and entry order is `Object.values(modelDef.contents)`
 * insertion order — not guaranteed to put a clean entry point before a
 * renamed/excepted/accepted one:
 *  1. Compile+classify every entry. A `rejected` classification throws
 *     immediately UNLESS this entry carries no `#(authorize)` note of its
 *     own (`options.authorizeOwnNotes` empty for `sourceName` — a
 *     `query_source` struct, which has no `annotations` whatsoever, is
 *     always this case): the probed text there was synthesized by
 *     concatenating ancestor and composite-resolved base gates
 *     (`effectiveAncestorGateExprs`), so a grammar violation in it is not an
 *     authoring mistake AT THIS entry point, and it is warned via
 *     `onRowLevelGateUnexpressible` instead of failing the load — the same
 *     escape pass 2 uses for a note-less PENDING failure, since
 *     `Model.resolveGateShape` denies every request against this shape
 *     regardless. An entry that DOES carry its own note still throws:
 *     the compiled condition's SHAPE (an `and`/`or`/`inGiven` tree) does not
 *     depend on which entry point it was probed from, only on whether it
 *     compiles there at all, so a grammar violation in an author's OWN gate
 *     is invalid wherever it is found, full stop. A `row_level` classification
 *     counts as this entry SUCCEEDING — its own
 *     `#(authorize)`-tagged note objects (`options.authorizeOwnNotes`, from
 *     `extractSourcesFromModelDef`) are added to a set of "proven" note
 *     objects. A compile failure is recorded as PENDING (source name, exprs,
 *     the underlying error) instead of decided yet.
 *  2. For each pending failure: it escapes — reported via
 *     {@link recordRowLevelGateRejected} (`onRowLevelGateRejected`) and
 *     `onRowLevelGateUnexpressible` for a human-readable warning (this
 *     module stays free of both the telemetry stack and a logger, see the
 *     module doc) — WITHOUT failing the load, if its own note objects are
 *     empty (no annotation of its own to have gotten wrong) or intersect the
 *     "proven" set from pass 1. Otherwise it falls back to the one-row
 *     probe — either the gate is genuinely broken (reports today's familiar
 *     error), or the given is not visible from this entry point's model at
 *     all: a gate INHERITED from a base is authored in the base's own given
 *     namespace, and Malloy merges only one level of import, so a base two
 *     or more hops away can reference a given this model cannot see. The
 *     probe reports that as an unresolved given rather than misreporting it
 *     as a broken gate.
 *
 * Throws `ModelCompilationError` naming the source on the first invalid
 * annotation. Shared by `Model.create` and the package-load worker so both
 * compile paths validate identically.
 */
export async function validateAuthorizeProbes(
   // Widened from `AuthorizeProbeCompiler` to also RUN a probe, not just
   // compile one: `assertNoVacuousDefaultAtom` (below) evaluates a
   // `row_level` gate's literal atom against its given's declared default,
   // which needs `.run()`, not just `.getPreparedQuery()`. Both real callers
   // (`Model.create`, the package-load worker) already pass a full
   // `ModelMaterializer`, which supports both.
   compiler: AuthorizeProbeCompiler & AuthorizeProbeExecutor,
   options: {
      authorizeMap?: AuthorizeMap;
      declaredTypes?: Map<string, string>;
      /**
       * Given name → declared default (rendered Malloy source text, from
       * `ApiGiven.default` / `malloyGivenToApi`). Passed to
       * {@link classifyAuthorizeGate} so a field-vs-given row-level
       * comparison can be refused when its given carries one — see that
       * function's doc for why a declared default is a vacuous-admission
       * hazard there in a way a probe can't catch structurally.
       */
      declaredDefaults?: Map<string, string>;
      /**
       * source name → the source's OWN-level `#(authorize)`-tagged note
       * objects (from `extractSourcesFromModelDef`) — empty when the struct
       * carries no annotation of its own at all (e.g. a `query_source`,
       * which has no `annotations` by construction). This is the
       * note-object identity the two-pass escape decision below keys on —
       * see the function doc for why gate TEXT is not safe for this and
       * object identity is what's left.
       */
      authorizeOwnNotes?: Map<string, AnnotationNote[]>;
      onRowLevelGateRejected?: (cause: RowLevelGateRejectionCause) => void;
      /**
       * Called instead of throwing when a gate that validated successfully at
       * some OTHER entry point fails to resolve at `sourceName` specifically
       * — see the doc above. `detail` is the underlying compile failure
       * (Malloy already names the unresolved field/join in it, e.g. `'org_id'
       * is not defined`), for a caller with a logger to report against.
       */
      onRowLevelGateUnexpressible?: (
         sourceName: string,
         detail: string,
      ) => void;
   },
): Promise<void> {
   const declaredTypes = options.declaredTypes ?? new Map<string, string>();
   const declaredDefaults =
      options.declaredDefaults ?? new Map<string, string>();
   // Note objects that validated successfully somewhere in this model — see
   // the function doc for why this, not gate TEXT, is the safe discriminator
   // for "is a pending failure elsewhere's genuinely inherited copy".
   const provenNoteObjects = new Set<AnnotationNote>();
   const ownNotesOf =
      options.authorizeOwnNotes ?? new Map<string, AnnotationNote[]>();
   const pending: Array<{ sourceName: string; exprs: string[]; err: unknown }> =
      [];

   // One `sourceName` may carry MORE THAN ONE group — see `AuthorizeMap`'s
   // doc. Each group is classified and validated INDEPENDENTLY, exactly as a
   // single-group source always was: that independence is what keeps two
   // groups' AND semantics intact instead of silently flattening them into
   // one OR'd disjunction (the bug this grouping exists to fix).
   for (const [sourceName, groups] of options.authorizeMap ?? []) {
      for (const exprs of groups) {
         if (exprs.length === 0) continue;

         let condition: CompiledGateCondition;
         try {
            condition = await liftRowLevelCondition(
               compiler,
               sourceName,
               exprs,
            );
         } catch (err) {
            pending.push({ sourceName, exprs, err });
            continue;
         }

         const classification = classifyAuthorizeGate(
            condition,
            declaredTypes,
            declaredDefaults,
         );
         // Whether the compiled gate reads any row field, per Malloy's own
         // reference-tracking walker. A field-LESS gate (`$ROLE like 'ana%'`,
         // `1 = 1`, `$ROLE_D != 'blocked'`) was a whole-source boolean before
         // every gate became a row filter, and it published under the looser
         // rules that governed one; the row-level grammar can refuse it now.
         // Failing the load for that turns the whole model FILE into a
         // compilation-failure placeholder, taking every ungated source in it
         // out of service too, so it takes the `onRowLevelGateUnexpressible`
         // escape instead: warn, and let this ONE entry point deny every
         // request (`resolveGateShape`). A field-READING gate is one the
         // row-level grammar already governed, so it still fails the load.
         const fieldUsage = condition.refSummary?.fieldUsage;
         const readsRowField =
            Array.isArray(fieldUsage) && fieldUsage.length > 0;
         if (classification.shape === "rejected") {
            options.onRowLevelGateRejected?.(classification.cause);
            // A source-level gate concatenates its own ancestor gates with
            // its composite-resolved base gates into one probed expression
            // list (`effectiveAncestorGateExprs`) before it ever reaches
            // this function, so the text rejected here was not necessarily
            // authored by `sourceName` itself. When this entry carries no
            // annotation of its own at all — a `query_source` struct has no
            // `annotations` by construction — the rejection cannot be
            // blamed on an author sitting at this entry point; it is
            // exactly the synthesized-predicate case, not a hand-written
            // bad gate. Route it the same way pass 2 already routes a
            // note-less compile failure: warn and deny at runtime
            // (`Model.resolveGateShape` still refuses every request against
            // this shape) instead of failing the whole load.
            const ownNotes = ownNotesOf.get(sourceName) ?? [];
            if (ownNotes.length === 0 || !readsRowField) {
               options.onRowLevelGateUnexpressible?.(
                  sourceName,
                  classification.detail,
               );
               continue;
            }
            throw new ModelCompilationError({
               message:
                  `Invalid #(authorize) annotation on source "${sourceName}" ` +
                  `[${exprs.join(" | ")}]: ${classification.detail}`,
            });
         }
         // shape === "row_level": the probe compiled at this entry point, so
         // the gate's field(s) resolved here. Still verify any literal atom
         // it carries isn't vacuously true at its given's declared default
         // before calling it valid — see `assertNoVacuousDefaultAtom`'s doc.
         //
         // Deliberately WITHOUT the `ownNotes.length === 0` escape the
         // `rejected` branch above has. That escape exists because a rejected
         // SHAPE at a note-less entry point is not an authoring mistake anyone
         // made here — the text was synthesized by concatenating ancestors'
         // gates. A vacuous default atom is the opposite: it is a property of
         // the DECLARED gate and its given's declared default, both authored
         // somewhere, and it is equally wrong at every entry point the gate
         // reaches. Escaping here would let a gate that admits every row for a
         // defaulting caller load as long as the first entry point that probes
         // it happens to carry no note of its own.
         //
         // What that costs, and it is the whole cost: the error names
         // `sourceName` — this entry point — which for a note-less entry is not
         // the source that authored the gate. Neither reviewer could construct a
         // spurious load FAILURE from it, so the effect is a correct refusal with
         // a misleading name in it. Pinned by the entry-point-naming test in
         // `row_level_authorize.integration.spec.ts`.
         //
         // The FIELD-LESS escape the `rejected` branch above takes does NOT
         // apply here either, and for a sharper reason than the note-less
         // one: that escape is only fail-closed because `resolveGateShape`
         // re-runs the SAME shape classification per request and rejects
         // independently. A vacuous default atom has no request-time
         // counterpart — it is found by PROBING, which the request path does
         // not do — so warning instead of throwing would leave the source
         // serving and admitting every row to a caller who supplies nothing.
         // A gate that admits everyone by default is broken rather than
         // merely unexpressible, so it fails the load.
         try {
            await assertNoVacuousDefaultAtom(
               compiler,
               sourceName,
               classification.literalAtoms,
            );
         } catch (err) {
            options.onRowLevelGateRejected?.("vacuous_default_atom");
            throw err;
         }
         for (const note of ownNotesOf.get(sourceName) ?? []) {
            provenNoteObjects.add(note);
         }
      }
   }

   for (const failure of pending) {
      const ownNotes = ownNotesOf.get(failure.sourceName) ?? [];
      // Genuinely inherited: either this entry carries no annotation of its
      // own at all (nothing here could be the authoring mistake — e.g. a
      // `query_source` struct, which has no `annotations` whatsoever), or
      // EVERY one of its own notes is, BY REFERENCE, a note that validated
      // successfully elsewhere. Gate TEXT is deliberately not consulted: two
      // sources can independently type the identical string and get two
      // SEPARATE note objects, and text matching cannot tell that apart from
      // a genuine by-reference inheritance — see the function doc.
      //
      // `every`, not `some`: a source can carry MORE THAN ONE of its own
      // notes (e.g. two `#(authorize)` annotations, OR'd) — one proven sound
      // elsewhere, the other its own genuinely broken gate. `some` reports
      // "inherited" as soon as ANY one note matches, so a source carrying
      // both would escape to a warning ("not expressible at this entry
      // point") that misdescribes its own broken annotation as merely
      // unexpressible here. `every` requires EVERY own note to trace back to
      // something already proven before it calls the whole entry inherited.
      const inherited =
         ownNotes.length === 0 ||
         ownNotes.every((note) => provenNoteObjects.has(note));
      if (inherited) {
         // Some OTHER entry point already proved this exact gate object
         // sound — this one specifically renamed, excluded, or projected
         // away the field(s) it needs. Not a reason to fail the whole
         // model: `Model.resolveGateShape` denies every request against
         // THIS entry point (see its doc), so leaving the load to continue
         // does not leak anything.
         const detail =
            failure.err instanceof Error
               ? failure.err.message
               : String(failure.err);
         options.onRowLevelGateRejected?.("entry_point_unexpressible");
         options.onRowLevelGateUnexpressible?.(failure.sourceName, detail);
         continue;
      }
      // No ancestor to blame this on — either genuinely broken, or an
      // independently-authored gate that merely shares text with something
      // else that validated. Fall back to the one-row probe.
      await runOneRowProbeOrThrow(compiler, failure.sourceName, failure.exprs);
   }
}

/**
 * Parse a single annotation string into its authorize expression.
 *
 * Returns the inner expression for a well-formed `#(authorize)` / `##(authorize)`
 * annotation, `null` if the string is not an authorize annotation at all, and
 * throws if it looks like one but is malformed (missing quotes, mismatched
 * quotes, or an empty body). The throw is what later compile-time validation
 * turns into a model-load error.
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
   return unwrapQuotedExpression(content.trim());
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

/**
 * Strip exactly one layer of wrapping double quotes off the annotation body and
 * return the inner expression. Inner single quotes are part of the expression
 * and pass through untouched; `\"` and `\\` inside the string are unescaped.
 */
function unwrapQuotedExpression(body: string): string {
   if (body.length < 2 || body[0] !== '"') {
      throw new Error(
         `authorize annotation expression must be a double-quoted string, got: ${body || "(empty)"}`,
      );
   }

   let expr = "";
   let i = 1;
   let closed = false;
   for (; i < body.length; i++) {
      const ch = body[i];
      if (ch === "\\" && i + 1 < body.length) {
         const next = body[i + 1];
         if (next === '"' || next === "\\") {
            expr += next;
            i++;
            continue;
         }
      }
      if (ch === '"') {
         closed = true;
         i++;
         break;
      }
      expr += ch;
   }

   if (!closed) {
      throw new Error(`authorize annotation has mismatched quotes: ${body}`);
   }
   const rest = body.slice(i).trim();
   if (rest.length > 0) {
      throw new Error(
         `authorize annotation has unexpected content after the expression: ${rest}`,
      );
   }
   if (expr.trim().length === 0) {
      throw new Error("authorize annotation has an empty expression body");
   }
   return expr;
}
