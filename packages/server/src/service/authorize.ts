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
 * This module parses, collects, and (at compile time) validates authorize
 * annotations. Evaluating the expression — the actual access gate — reuses
 * `buildAuthorizeProbe`. Kept light so it bundles cleanly into the
 * package-load worker (its only non-type import is `../errors`).
 */

import { type GivenValue } from "@malloydata/malloy";
import { BadRequestError, ModelCompilationError } from "../errors";
import { type AnnotationNote } from "./annotations";

/**
 * An `authorize` annotation's opening tag, source-level (`#`) or file-level
 * (`##`), tolerant of whitespace inside the parens (`#( authorize )`).
 *
 * ONE pattern serves both the parser ({@link parseAuthorizeAnnotation}, anchored
 * below) and the caller-text rejection ({@link assertNoCallerAuthorizeAnnotation},
 * unanchored). They MUST accept the same spellings: a spelling the rejecter
 * catches but the parser ignores is an author-side fail-OPEN — the author reads
 * their source as locked while it carries no gate at all and loads without
 * complaint.
 */
const AUTHORIZE_TAG = String.raw`##?\(\s*authorize\s*\)`;
const AUTHORIZE_ANNOTATION_ANYWHERE = new RegExp(AUTHORIZE_TAG);
const AUTHORIZE_ANNOTATION_PREFIX = new RegExp(`^${AUTHORIZE_TAG}`);

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
 * Whether ANY of `texts` contains an authorize tag (`#(authorize)` /
 * `##(authorize)`), well-formed or not.
 *
 * Unlike {@link collectAuthorizeExprs}, this does not parse the body or throw
 * on a malformed one — it exists to DETECT the tag in a position nothing ever
 * reads it FROM at all (see {@link MisplacedAuthorizeAnnotation}), where the
 * fact worth catching is that the author wrote the tag, not whether its body
 * happens to parse.
 */
export function containsAuthorizeAnnotationTag(texts: string[]): boolean {
   return texts.some((text) => AUTHORIZE_ANNOTATION_ANYWHERE.test(text));
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

/** Human-readable position for a {@link MisplacedAuthorizeAnnotation}, shared
 *  between {@link assertNoMisplacedAuthorizeAnnotations} (fatal) and
 *  {@link describeMisplacedJoinAuthorizeWarnings} (non-fatal, join-only). */
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
 * Non-fatal counterpart to {@link assertNoMisplacedAuthorizeAnnotations} for a
 * `#(authorize)` on a `join_one:`/`join_many:` field that
 * `extractSourcesFromModelDef` could not explain as Malloy's by-reference copy
 * of a gated source's own note (`source_extraction.ts`'s
 * `joinFieldNamesUnresolvableDeclaration` and its identity check). A join line
 * has no enforcement effect either way, so this is worth telling the author
 * about — but the detection has twice proved unable to tell an authored
 * join-line annotation from Malloy's copy across an import boundary, and
 * getting that wrong here means refusing to load a whole package rather than
 * one misplaced annotation, so it warns instead of throwing. Returns one
 * formatted message per finding; callers route them through whatever
 * load-warning channel they already have (`logger.warn` in-process, the
 * worker's `authorizeWarnings` wire field out of process).
 */
export function describeMisplacedJoinAuthorizeWarnings(
   found: readonly MisplacedAuthorizeAnnotation[],
): string[] {
   return found.map(
      (f) =>
         `#(authorize) ${describeMisplacedAuthorizeAnnotation(f)} is a ` +
         `join_one:/join_many: line and is never enforced there; gating a ` +
         `join has no effect. If this was meant to lock access, move the ` +
         `annotation to the JOINED source's own \`source:\` declaration.`,
   );
}

/** source name → effective authorize expressions (file-level then source-level). */
export type AuthorizeMap = Map<string, string[]>;

/** A `given:` declaration to prepend to a probe so it compiles standalone. */
export interface ProbeGivenDecl {
   name: string;
   type: string;
}

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
 * `givenDecls`, when supplied, are prepended as the probe's OWN `given:`
 * block, making the probe self-contained: it compiles against whatever
 * givens it declares itself rather than depending on the ambient model's
 * given namespace. This is what lets {@link evaluateAuthorize} gate a joined
 * source whose givens live in a model that isn't the one the probe is
 * compiled against (see the two-hop transitive-import case in
 * `docs/authorize.md`). Compile-time validation (`validateAuthorizeProbes`)
 * calls this with no decls, so it still validates against the ambient
 * namespace of the model it's compiling in.
 */
export function buildAuthorizeProbe(
   exprs: string[],
   givenDecls: ProbeGivenDecl[] = [],
): string {
   const selects = exprs
      .map((expr, i) => `__auth_${i} is (${expr})`)
      .join("\n      ");
   const givenBlock =
      givenDecls.length > 0
         ? `given:\n${givenDecls.map((g) => `  ${g.name} :: ${g.type}`).join("\n")}\n\n`
         : "";
   return `${givenBlock}run: duckdb.sql("SELECT 1 AS __authorize_probe_row") -> {
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
 * How a gate expression is enforced.
 *
 *  - `given_only` — references no row field. Evaluated by the synthetic one-row
 *    DuckDB probe, unchanged: a whole-source boolean, exactly as every gate was
 *    before row-level gates existed.
 *  - `row_level` — references at least one field of the source, or of a source
 *    joined into it. Enforced as a row filter on the entry source. There is no
 *    boolean answer to fall back on, so every path that cannot apply the filter
 *    must deny.
 */
export type AuthorizeGateShape = "given_only" | "row_level";

/**
 * Why a row-level gate was refused at publish. Also the metric label.
 *
 * The first five come from {@link classifyAuthorizeGate}: the gate's compiled
 * condition IS readable and is not an allowed shape — invalid IN ITSELF,
 * wherever it is probed from. `entry_point_unexpressible` is different in
 * kind: the gate is a valid, allowed shape, but at one entry point that did
 * not itself declare it — a derived source (an `extend` that
 * renamed/excluded/projected away the field, or a `query_source` projection)
 * inheriting a source's gate, or a source sharing a FILE-level `##(authorize)`
 * with a sibling that resolves it — the field it reads did not resolve at
 * all, so there was no condition to classify. That case does not fail the
 * load — see `validateAuthorizeProbes`'s doc, in particular for how it
 * confirms the gate is genuinely inherited rather than an
 * independently-authored one that merely shares text with something else: by
 * the gate's own annotation NOTE OBJECT, not its string.
 */
export type RowLevelGateRejectionCause =
   | "array_given_needs_in"
   | "scalar_given_rejects_in"
   | "unsupported_node"
   | "no_given_reference"
   | "unreachable_given"
   | "entry_point_unexpressible";

export type RowLevelGateClassification =
   | { shape: "given_only" }
   | { shape: "row_level"; fieldPaths: string[][]; givenNames: string[] }
   | { shape: "rejected"; cause: RowLevelGateRejectionCause; detail: string };

/**
 * The compiled-condition node kinds a row-level gate may be built from, and
 * how to descend each one.
 *
 * A gate is an access-control rule, so the set of shapes it may take should be
 * small enough to read in one screen. Everything absent from this table is
 * refused at publish — a function call, arithmetic, a literal comparison, a
 * `like`, a bare `true`. Widening it is a decision someone makes on purpose.
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
 * givens, so `refSummary.fieldUsage` answers "does this gate reference a row
 * field" with no guessing — the one question that decides which enforcement
 * mechanism the gate gets. A text scan cannot answer it: `$ROLE` and `region`
 * are both bare words, and being wrong in either direction is a security bug
 * (a field-referencing gate sent to the one-row probe never admits anybody; a
 * given-only gate sent to the row filter changes the meaning of every gate
 * already published).
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
 * Fails CLOSED: an unreadable condition is a rejection, never a pass.
 */
export function classifyAuthorizeGate(
   condition: CompiledGateCondition,
   declaredTypes: Map<string, string>,
): RowLevelGateClassification {
   const fieldUsage = condition.refSummary?.fieldUsage;
   // Malloy's own reference-tracking walker populated this. Absent or empty
   // means the gate reads no row field, which is the pre-existing whole-source
   // boolean — it MUST keep the probe, or every published gate changes
   // enforcement mechanism on upgrade.
   if (!Array.isArray(fieldUsage) || fieldUsage.length === 0) {
      return { shape: "given_only" };
   }
   const fieldPaths: string[][] = [];
   const givenNames: string[] = [];
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
         return walkFieldOperand(n.e);
      }
      if (SCALAR_COMPARISON_NODES.has(kind)) {
         const kids = asNode(n.kids);
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
            // gate; it contributes no field reference of its own, so it
            // doesn't get pushed to `fieldPaths`.
            givenNames.push(given);
            return true;
         }
         givenNames.push(given);
         return walkFieldOperand(otherSide);
      }
      return reject(
         "unsupported_node",
         `\`${kind}\` is not permitted in a gate; a row-level gate is a ` +
            `boolean combination of \`<field> <operator> $GIVEN\` comparisons`,
      );
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
      fieldPaths.push(n.path.map(String));
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
   if (givenNames.length === 0) {
      return {
         shape: "rejected",
         cause: "no_given_reference",
         detail:
            "a row-level gate must compare a field against a given; a gate " +
            "that references none is a fixed filter and belongs in the " +
            "source's own `where:`",
      };
   }
   return { shape: "row_level", fieldPaths, givenNames };
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
 * Infer a Malloy given type from a caller-supplied JS value, so a
 * self-contained probe can declare exactly the givens an expression
 * references without depending on the ambient model's own given namespace —
 * which a joined source pulled in from a different (possibly multi-hop
 * transitively imported) file may not share. Returns `null` for a value with
 * no sensible Malloy type (null, a plain object, an empty untyped array);
 * callers skip declaring that given, which fails the probe closed —
 * consistent with the existing "referenced given has no value" behavior.
 */
function inferGivenType(value: GivenValue): string | null {
   if (typeof value === "string") return "string";
   if (typeof value === "number" || typeof value === "bigint") return "number";
   if (typeof value === "boolean") return "boolean";
   if (value instanceof Date) return "timestamp";
   if (Array.isArray(value)) {
      if (value.length === 0) return null;
      const elementType = inferGivenType(value[0]);
      return elementType ? `${elementType}[]` : null;
   }
   return null;
}

/**
 * Build the self-contained `given:` declarations + bound values a probe
 * needs for one expression, from whatever the caller supplied. A referenced
 * name that's absent from `givens` (or whose value has no inferrable type) is
 * simply left undeclared — the probe then fails to compile for that
 * expression (an undeclared `$NAME`), which {@link evaluateAuthorize} already
 * treats as "this disjunct can't be evaluated" and denies that branch.
 *
 * `declaredTypes`, when supplied, is preferred over inferring the type from
 * the caller's JS value — the gate author's own `given:` declaration is the
 * source of truth (e.g. `$LEVEL > 3` should compare numerically even if the
 * caller sends `"5"` as a string). Only covers a name declared within one
 * import hop of the entry model (the same reach as `this.givens` — see
 * `docs/authorize.md`); a gate on a source reached through a deeper
 * transitive import falls back to inferring from the value, same as before.
 */
function bindProbeGivens(
   expr: string,
   givens: Record<string, GivenValue>,
   declaredTypes?: Map<string, string>,
): { decls: ProbeGivenDecl[]; bound: Record<string, GivenValue> } {
   const decls: ProbeGivenDecl[] = [];
   const bound: Record<string, GivenValue> = {};
   for (const name of referencedGivenNames(expr)) {
      if (!(name in givens)) continue;
      const value = givens[name];
      const type = declaredTypes?.get(name) ?? inferGivenType(value);
      if (!type) continue;
      decls.push({ name, type });
      bound[name] = value;
   }
   return { decls, bound };
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
 * react — {@link evaluateAuthorize} treats it as "can't evaluate this way,
 * try something else" rather than granting.
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
 * Evaluate a source's authorize disjunction against the supplied givens.
 * Returns true if ANY expression evaluates true (OR semantics).
 *
 * Each expression is probed INDEPENDENTLY rather than batched into one query.
 * That is what preserves OR semantics: an expression that can't be evaluated —
 * e.g. it references a given the request didn't supply, so Malloy throws "no
 * value" — counts as "does not grant" (false), and the next disjunct is still
 * tried. Batching them would let a missing given in one unused branch throw and
 * sink the whole request (denying an admin who matched a different branch).
 *
 * Per-branch failures are swallowed to false (fail closed at the branch level);
 * access is granted only if some branch genuinely returns true. Short-circuits
 * on the first true. Returns false (→ caller denies) if none grant.
 *
 * Each expression is tried TWICE if needed, in one of two orders depending on
 * `selfContainedFirst`:
 *
 *  - AMBIENT FIRST (`selfContainedFirst` false — the default, used only for
 *    the run target's OWN source gate): compile against `executor`'s own
 *    given namespace, passing the full supplied `givens` — the original
 *    approach, unchanged for the common case where `executor` (always the
 *    ENTRY model's materializer) already has the referenced givens in scope
 *    (same file, or a directly imported one — Malloy merges one level of
 *    import into a model's given namespace). Falls back to the
 *    self-contained probe below only if the ambient probe THROWS.
 *
 *  - SELF-CONTAINED FIRST (`selfContainedFirst` true — used for every gate
 *    reached via a join/derivation/composite-member walk): declare just the
 *    givens this expression references ({@link bindProbeGivens}), preferring
 *    `declaredTypes` (the gate author's own declaration) over inferring from
 *    the caller-supplied value, so the probe compiles independently of
 *    `executor`'s namespace. This is what isolates a JOINED source's gate
 *    from the entry model's own ambient given namespace: if the entry model
 *    happens to declare its OWN given of the SAME NAME the joined gate
 *    references, an ambient-first probe would compile successfully against
 *    the ENTRY model's default/value instead of failing closed — silently
 *    granting access based on an unrelated given. An expression referencing
 *    NO givens at all (a constant/public gate, e.g. `#(authorize) "true"`)
 *    has nothing ambient to isolate from, so it's evaluated with a no-decls
 *    self-contained probe and returned directly — no fallback needed.
 *    Otherwise, the probe is attempted ONLY when EVERY referenced given got
 *    a decl — a probe that declares just SOME of them is not actually
 *    isolated: `executor.loadQuery` still compiles it against the ENTRY
 *    model's own materializer, so an undeclared referenced name resolves via
 *    the entry model's own ambient value/default for that name INSTEAD OF
 *    THROWING — reopening the collision hole for a partially supplied
 *    multi-given gate without ever reaching a catch block. So "not every
 *    referenced name could be bound" denies immediately, with NO probe
 *    attempted and NO ambient fallback. Once every referenced name IS
 *    declared, ambient is tried as a last resort only if that fully-declared
 *    probe itself throws for some OTHER reason — safe at that point since no
 *    referenced name is left for an entry default to decide.
 *
 * Either order still lets a gate on a source reached through a multi-hop
 * transitive import evaluate correctly — Malloy does not flatten a `given:`
 * declaration through more than one level of import, so the entry model's own
 * namespace can be missing a given that's declared two-or-more imports away
 * from a JOINED source's home file (see `docs/authorize.md`).
 */
export async function evaluateAuthorize(
   executor: AuthorizeProbeExecutor,
   exprs: string[],
   givens: Record<string, GivenValue>,
   declaredTypes?: Map<string, string>,
   options?: { selfContainedFirst?: boolean },
): Promise<boolean> {
   const selfContainedFirst = options?.selfContainedFirst ?? false;
   for (const expr of exprs) {
      if (selfContainedFirst) {
         if (
            await evaluateSelfContainedFirst(
               executor,
               expr,
               givens,
               declaredTypes,
            )
         ) {
            return true;
         }
         continue;
      }
      try {
         if (await runProbe(executor, buildAuthorizeProbe([expr]), givens)) {
            return true;
         }
         continue;
      } catch {
         // Ambient compile/eval failed — fall through to the self-contained
         // retry below rather than immediately treating this disjunct as
         // not-granting.
      }
      try {
         const { decls, bound } = bindProbeGivens(expr, givens, declaredTypes);
         if (decls.length === 0) continue; // nothing left to try — deny
         if (
            await runProbe(executor, buildAuthorizeProbe([expr], decls), bound)
         ) {
            return true;
         }
      } catch {
         // Still can't be evaluated — deny this branch and try the next.
         // Does not fail the whole request, which is what keeps OR
         // semantics intact.
         continue;
      }
   }
   return false;
}

/**
 * One expression's self-contained-first evaluation (see
 * {@link evaluateAuthorize}'s `selfContainedFirst` mode). A probe is only
 * ever attempted once every given the expression references has been
 * declared — a partially-declared probe still compiles against the ENTRY
 * model's own materializer, so an undeclared name silently resolves via the
 * entry's own ambient value/default instead of throwing, which is not
 * isolation at all. Ambient is tried as a last resort only when the
 * fully-declared self-contained probe itself throws for some other reason.
 */
async function evaluateSelfContainedFirst(
   executor: AuthorizeProbeExecutor,
   expr: string,
   givens: Record<string, GivenValue>,
   declaredTypes?: Map<string, string>,
): Promise<boolean> {
   const referenced = referencedGivenNames(expr);
   if (referenced.length === 0) {
      // The expression references NO givens at all — a constant/public gate
      // (e.g. `#(authorize) "true"`). There's nothing ambient to isolate
      // from, so run the no-decls probe (still fully self-contained: it
      // declares no givens and is handed none) and use its result directly.
      // This is NOT the "unsatisfiable" case below — that's specifically
      // about a referenced given the caller can't supply.
      try {
         return await runProbe(executor, buildAuthorizeProbe([expr]), {});
      } catch {
         return false;
      }
   }
   const { decls, bound } = bindProbeGivens(expr, givens, declaredTypes);
   if (decls.length !== referenced.length) {
      // Not every referenced given could be bound — deny. A probe that
      // declares only SOME of the referenced names is not actually isolated:
      // `executor.loadQuery` compiles it against the ENTRY model's own
      // materializer, so an undeclared referenced given resolves via the
      // ENTRY model's own ambient value/default for that name instead of
      // throwing — reopening the name-collision hole for a partially
      // supplied multi-given gate. Only attempt a probe once every
      // referenced name can be declared (and thus shadowed) ourselves.
      return false;
   }
   try {
      return await runProbe(
         executor,
         buildAuthorizeProbe([expr], decls),
         bound,
      );
   } catch {
      // Every referenced given was supplied and declared, so a throw here
      // isn't the collision case — there's no unsupplied name left for an
      // entry default to decide. Ambient is a safe last resort.
      try {
         return await runProbe(executor, buildAuthorizeProbe([expr]), givens);
      } catch {
         return false;
      }
   }
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
 * `__auth_N` and `Model.liftGateCondition`'s identical probe shape (kept in
 * lockstep with it: both need the SAME shape to read back the compiled
 * `FilterCondition` from `_query.structRef.filterList`).
 */
function buildRowLevelProbe(sourceName: string, exprs: string[]): string {
   const filterText = exprs.map((e) => `(${e})`).join(" or ");
   return `run: ${quoteMalloyIdentifier(sourceName)} extend { where: ${filterText} } -> { select: __authorize_probe is 1; limit: 1 }`;
}

/**
 * Compile {@link buildRowLevelProbe} and lift the LAST entry of the compiled
 * query's `structRef.filterList` — the condition Malloy built for the
 * `where:` this probe just added. Throws if the probe fails to compile (an
 * unreachable given, or — the case this exists to catch — a field the gate
 * references that this entry point renamed, excluded, or projected away) or
 * if the compiled shape carries no filter at all.
 */
async function liftRowLevelCondition(
   compiler: AuthorizeProbeCompiler,
   sourceName: string,
   exprs: string[],
): Promise<CompiledGateCondition> {
   const prepared = (await compiler
      .loadQuery(buildRowLevelProbe(sourceName, exprs))
      .getPreparedQuery()) as {
      _query?: { structRef?: { filterList?: CompiledGateCondition[] } };
   };
   const filterList = prepared._query?.structRef?.filterList;
   if (!Array.isArray(filterList) || filterList.length === 0) {
      throw new Error(
         `row-level probe for "${sourceName}" carries no filter condition`,
      );
   }
   return filterList[filterList.length - 1];
}

/**
 * Run the one-row `buildAuthorizeProbe`, wrapping a failure into the
 * `ModelCompilationError` shape `validateAuthorizeProbes` has always thrown.
 * Shared by the `given_only` path and the "no ancestor to blame this on"
 * fallback so a genuinely broken gate reports the identical message
 * regardless of which path found it.
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
 * Translation-time validation. Type mismatches such as `$ROLE = 5` are NOT
 * Malloy compile errors, so they are not caught here — they fail closed at
 * the runtime gate.
 *
 * Validation is shape-aware: it works from `options.authorizeMap` (source
 * name → EFFECTIVE gates, inheritance included, from
 * `extractSourcesFromModelDef`) — the full entry-point list, not just the
 * declaring source. `docs/row-level-authorize-spike-findings.md` §4 measured
 * that a gate's field reference can resolve at the source that declares it
 * and still fail at an entry point that renamed, excluded, or projected the
 * field away (`rename:`, `except:`, `accept:`, a `->` projection) — and that
 * loading the model successfully is NOT evidence this can't happen; the break
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
 *     - `given_only` — ALSO run the pre-existing one-row probe, unchanged,
 *       so a gate that has always been a whole-source boolean keeps being
 *       validated exactly as it always was.
 *     - `row_level` — the field(s) it reads resolved at this entry point;
 *       valid, nothing further to do.
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
 * `options.authorizeOwnNotes` (from `extractSourcesFromModelDef`) folds a
 * FILE-level `##(authorize)`'s own note object into every source's entry —
 * the same object for all of them, since there is exactly one file-level
 * annotation list — for the identical reason: a file-level gate is authored
 * once but applies everywhere, so proving it sound at one source has to
 * downgrade it everywhere else it fails, not just for a source-level gate.
 *
 * TWO PASSES over `authorizeMap`, because that answer isn't known until every
 * entry has been tried, and entry order is `Object.values(modelDef.contents)`
 * insertion order — not guaranteed to put a clean entry point before a
 * renamed/excepted/accepted one:
 *  1. Compile+classify every entry. A `rejected` classification throws
 *     immediately, unconditionally — the compiled condition's SHAPE (an
 *     `and`/`or`/`inGiven` tree) does not depend on which entry point it was
 *     probed from, only on whether it compiles there at all, so a grammar
 *     violation is invalid wherever it is found, full stop. `given_only`
 *     and `row_level` both count as this entry SUCCEEDING — its own
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
   compiler: AuthorizeProbeCompiler,
   options: {
      authorizeMap?: AuthorizeMap;
      declaredTypes?: Map<string, string>;
      /**
       * source name → the source's OWN-level `#(authorize)`-tagged note
       * objects (from `extractSourcesFromModelDef`) PLUS the file-level
       * `##(authorize)` note objects, which every source carries the SAME
       * object for — empty only when the struct carries no annotation of its
       * own at all AND the file declares no `##(authorize)` (e.g. a
       * `query_source` in a file with no file-level gate). This is the
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
   // Note objects that validated successfully somewhere in this model — see
   // the function doc for why this, not gate TEXT, is the safe discriminator
   // for "is a pending failure elsewhere's genuinely inherited copy".
   const provenNoteObjects = new Set<AnnotationNote>();
   const ownNotesOf =
      options.authorizeOwnNotes ?? new Map<string, AnnotationNote[]>();
   const pending: Array<{ sourceName: string; exprs: string[]; err: unknown }> =
      [];

   for (const [sourceName, exprs] of options.authorizeMap ?? []) {
      if (exprs.length === 0) continue;

      let condition: CompiledGateCondition;
      try {
         condition = await liftRowLevelCondition(compiler, sourceName, exprs);
      } catch (err) {
         pending.push({ sourceName, exprs, err });
         continue;
      }

      const classification = classifyAuthorizeGate(condition, declaredTypes);
      if (classification.shape === "given_only") {
         await runOneRowProbeOrThrow(compiler, sourceName, exprs);
         for (const note of ownNotesOf.get(sourceName) ?? []) {
            provenNoteObjects.add(note);
         }
         continue;
      }
      if (classification.shape === "rejected") {
         options.onRowLevelGateRejected?.(classification.cause);
         throw new ModelCompilationError({
            message:
               `Invalid #(authorize) annotation on source "${sourceName}" ` +
               `[${exprs.join(" | ")}]: ${classification.detail}`,
         });
      }
      // shape === "row_level": the probe compiled at this entry point, so
      // the gate's field(s) resolved here. Valid.
      for (const note of ownNotesOf.get(sourceName) ?? []) {
         provenNoteObjects.add(note);
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
 * count, so one prefix pattern covers both. It matches exactly what
 * {@link assertNoCallerAuthorizeAnnotation} rejects, including inner whitespace
 * (`#( authorize )`) — a spelling only one of them recognized would either
 * silently drop an author's gate or wrongly refuse a caller's query.
 */
export function parseAuthorizeAnnotation(annotation: string): string | null {
   const trimmed = annotation.trim();
   const prefix = AUTHORIZE_ANNOTATION_PREFIX.exec(trimmed);
   if (!prefix) return null;
   return unwrapQuotedExpression(trimmed.slice(prefix[0].length).trim());
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
