/**
 * `#(authorize)` / `##(authorize)` annotation parsing.
 *
 * Annotation format:
 *   #(authorize)  "<malloy-bool-expr>"   — source-level, on a single source
 *   ##(authorize) "<malloy-bool-expr>"   — file/model-level, applies to the file
 *
 * The body is a single double-quoted Malloy boolean expression that references
 * declared givens (`$NAME`), e.g. `#(authorize) "$ROLE = 'analyst'"`. The
 * expression itself routinely contains single quotes (Malloy string literals),
 * so we cannot reuse filter.ts's whitespace tokenizer — we unwrap exactly one
 * layer of double quotes and hand the inner expression back untouched.
 *
 * This module parses, collects, and (at compile time) validates authorize
 * annotations. Evaluating the expression — the actual access gate — lands in a
 * later PR and reuses `buildAuthorizeProbe`. Kept light so it bundles cleanly
 * into the package-load worker (its only non-type import is `../errors`).
 */

import { type GivenValue } from "@malloydata/malloy";
import { BadRequestError, ModelCompilationError } from "../errors";

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
   options?: { selfContainedFirst?: boolean; ambientPrefix?: number },
): Promise<boolean> {
   const selfContainedFirst = options?.selfContainedFirst ?? false;
   // The first `ambientPrefix` expressions are always probed ambient-first, even
   // in self-contained mode. They are the file-level `##(authorize)` gates, which
   // are authored in the ENTRY model — the one namespace where ambient resolution
   // is unambiguously right — while the rest of the list may have been carried in
   // from another source. Probing the file-level gate self-contained would drop
   // the entry model's own declared `given:` DEFAULTS, so a model-wide admin
   // override stopped working as soon as the source it applied to also inherited
   // a gate. Both live in ONE list because they are OR'd (either grants), so they
   // cannot be split into separate AND-ed entries.
   const ambientPrefix = options?.ambientPrefix ?? 0;
   for (const [index, expr] of exprs.entries()) {
      if (selfContainedFirst && index >= ambientPrefix) {
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

/** A source plus its effective authorize expressions. */
interface SourceWithAuthorize {
   name?: string;
   authorize?: string[];
}

/**
 * Translation-time validation. For each source carrying authorize expressions,
 * compile the probe (no givens, no DB execution) so unknown givens and
 * source-field references surface as compile errors at model load instead of
 * first request. Type mismatches such as `$ROLE = 5` are NOT Malloy compile
 * errors, so they are not caught here — they fail closed at the runtime gate.
 *
 * Pass the gates DECLARED on each source (`ownAuthorizeSources` from
 * `extractSourcesFromModelDef`), not its effective list. A gate INHERITED from a
 * base is authored in the base's own given namespace, and Malloy merges only one
 * level of import — so a base two or more hops away can reference a given this
 * model cannot see. Probing it here would fail the load with a 424 that blames a
 * perfectly good annotation. The inherited case is covered at request time
 * instead, fail-closed (`Model.gateExprsForOwnAnnotations` maps a parse failure
 * to a single unsatisfiable `"false"`).
 *
 * Throws `ModelCompilationError` naming the source on the first invalid
 * annotation. Shared by `Model.create` and the package-load worker so both
 * compile paths validate identically.
 */
export async function validateAuthorizeProbes(
   compiler: AuthorizeProbeCompiler,
   sources: readonly SourceWithAuthorize[],
): Promise<void> {
   for (const source of sources) {
      const exprs = source.authorize;
      if (!exprs || exprs.length === 0) continue;
      try {
         await compiler
            .loadQuery(buildAuthorizeProbe(exprs))
            .getPreparedQuery();
      } catch (err) {
         const detail = err instanceof Error ? err.message : String(err);
         throw new ModelCompilationError({
            message: `Invalid #(authorize) annotation on source "${source.name ?? "(unnamed)"}" [${exprs.join(" | ")}]: ${detail}`,
         });
      }
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
