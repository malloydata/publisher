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
 * Evaluate a source's authorize disjunction against the supplied givens by
 * running the probe. Returns true if ANY expression evaluates true (OR
 * semantics). A missing row counts as false. Throws if the probe itself fails
 * (e.g. a referenced given has no supplied value and no default) — the caller
 * treats any throw as denial (fail closed), which is how "missing given value"
 * becomes a 403 rather than a server error.
 */
export async function evaluateAuthorize(
   executor: AuthorizeProbeExecutor,
   exprs: string[],
   givens: Record<string, GivenValue>,
): Promise<boolean> {
   const result = await executor
      .loadQuery(buildAuthorizeProbe(exprs))
      .run({ rowLimit: 1, givens });
   const row = result?.data?.value?.[0];
   if (!row) return false;
   return exprs.some((_, i) => isProbeTrue(row[`__auth_${i}`]));
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
