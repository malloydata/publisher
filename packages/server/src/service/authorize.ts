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
 * into the package-load worker (the only non-type import is the shared
 * `ModelCompilationError`).
 */

import { type GivenValue } from "@malloydata/malloy";
import { ModelCompilationError } from "../errors";

const SOURCE_PREFIX = "#(authorize)";
const FILE_PREFIX = "##(authorize)";

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

/**
 * Given names an authorize expression references (`$NAME` tokens), deduped,
 * in first-seen order. Used to figure out which givens a self-contained
 * probe needs to declare for a given expression.
 */
export function referencedGivenNames(expr: string): string[] {
   const names: string[] = [];
   const seen = new Set<string>();
   for (const match of expr.matchAll(GIVEN_REF_PATTERN)) {
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
 * Each expression is tried TWICE if needed:
 *  1. Ambient: compile against `executor`'s own given namespace, passing the
 *     full supplied `givens` — the original approach, unchanged for the
 *     common case where `executor` (always the ENTRY model's materializer)
 *     already has the referenced givens in scope (same file, or a directly
 *     imported one — Malloy merges one level of import into a model's given
 *     namespace).
 *  2. Self-contained fallback ({@link bindProbeGivens}), tried only if (1)
 *     throws: declare just the givens this expression references, preferring
 *     `declaredTypes` (the gate author's own declaration) over inferring
 *     from the caller-supplied value, so the probe compiles
 *     independently of `executor`'s namespace. This is what lets a gate on a
 *     source reached through a multi-hop transitive import evaluate
 *     correctly — Malloy does not flatten a `given:` declaration through more
 *     than one level of import, so the entry model's own namespace can be
 *     missing a given that's declared two-or-more imports away from a
 *     JOINED source's home file (see `docs/authorize.md`). Skipped as a
 *     no-op (denies) when no referenced given has a suppliable value, same
 *     as when (1) fails for an ordinary "no value supplied" reason.
 */
export async function evaluateAuthorize(
   executor: AuthorizeProbeExecutor,
   exprs: string[],
   givens: Record<string, GivenValue>,
   declaredTypes?: Map<string, string>,
): Promise<boolean> {
   for (const expr of exprs) {
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
 */
export function parseAuthorizeAnnotation(annotation: string): string | null {
   const trimmed = annotation.trim();

   let body: string;
   // Check the file-level prefix first — `##(authorize)` also starts with `#`.
   if (trimmed.startsWith(FILE_PREFIX)) {
      body = trimmed.slice(FILE_PREFIX.length).trim();
   } else if (trimmed.startsWith(SOURCE_PREFIX)) {
      body = trimmed.slice(SOURCE_PREFIX.length).trim();
   } else {
      return null;
   }

   return unwrapQuotedExpression(body);
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
