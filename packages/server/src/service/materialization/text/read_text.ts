// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The Malloy that declares a source over a stored table: the build companion
 * of a file's persisted views and named queries, a rollup's build companion
 * and its merged measures, the companion URLs beside a declaring file, and
 * the notes a variant carries onto a view it redeclares.
 *
 * Shared by the planner, which compiles the companions at load to validate
 * them and address their tables, and the rollup and view legs, which compile
 * the same text at query time to route a query to the table. Every name and
 * value is written through `malloy_text`; the author's own `#@` lines and
 * notes are copied verbatim, single lines only.
 *
 * Pure.
 */

import * as path from "path";
import { fileURLToPath } from "url";
import { annotationTexts, type AnnotationsDef } from "../../annotations";
import { byCodepoint, routeOfNote } from "../../annotation_reader";
import {
   accessLiftEdits,
   applyEdits,
   TERM_NOT_FOUND,
   termSpans,
   type TextEdit,
} from "./declaration_text";
import { offsetOf } from "./malloy_lexer";
import {
   annotationValue,
   doubleQuotedString,
   functionName,
   identifier,
   numberLiteral,
   stringLiteral,
} from "../../malloy_text";
import type {
   BinaryOp,
   MeasureExpr,
   Reaggregate,
} from "../domain/rollup_measures";
import {
   PERSISTED_VIEW_COMPANION_SUFFIX,
   splitSourceID,
   type PersistedQueryPlan,
   type PersistedViewPlan,
   type RollupVariantPlan,
} from "../domain/table_plans";

/** The companion URL for a declaring file, as an `href`. */
export function persistedViewCompanionHref(declaringURL: string): string {
   return `${declaringURL}${PERSISTED_VIEW_COMPANION_SUFFIX}`;
}

/** The alias the companion imports a declaring source under. */
function baseAlias(source: string): string {
   return `${source}__pv_base`;
}

/**
 * The companion's text for the views `plans` declares, all in the file
 * imported as `importPath`, with `extra`'s imports and sources after theirs
 * (the persisted queries', see {@link generatePersistedResultFile}), or
 * undefined when there are none.
 */
export function generatePersistedViewFile(
   plans: readonly PersistedViewPlan[],
   importPath: string,
   extra?: { imports: string; sources: string },
   /** Another file a build source's source is imported from, by build sourceID. */
   importOf: (buildSourceID: string) => string | undefined = () => undefined,
): string | undefined {
   if (plans.length === 0 && !extra) return undefined;
   const sources = [...new Set(plans.map((p) => p.sourceName))].sort(
      byCodepoint,
   );
   const viewImports = sources.map((s) => {
      const first = plans.find((p) => p.sourceName === s);
      const from = (first && importOf(first.buildSourceID)) ?? importPath;
      return `import { ${identifier(baseAlias(s))} is ${identifier(s)} } from ${doubleQuotedString(from)}`;
   });
   const viewSources = plans.map(
      (p) =>
         // malloy-text: the author's own `#@` lines, verbatim (single lines, no `@env.`; see planSourcePersistedViews)
         `${p.persistLines.join("\n")}
source: ${identifier(p.buildSourceName)} is ${identifier(baseAlias(p.sourceName))} -> ${identifier(p.viewName)}`,
   );
   // malloy-text: import lines built through identifier and doubleQuotedString
   const imports = [...viewImports, ...(extra ? [extra.imports] : [])].join(
      "\n",
   );
   // malloy-text: build sources named through identifier, under the author's own `#@` lines
   const sourcesText = [...viewSources, ...(extra ? [extra.sources] : [])].join(
      "\n\n",
   );
   return `##! experimental.persistence
${imports}

${sourcesText}
`;
}

/**
 * The companion's lines for the queries `plans` declares, all in the file
 * imported as `importPath`: one import of each query and its named run
 * target, then one build source per query.
 *
 * The run target is imported beside the query because a selectively imported
 * named query keeps its source as a bare name and resolves it in the
 * importing model, so `{ q }` alone does not compile.
 */
function persistedQueryCompanionParts(
   plans: readonly PersistedQueryPlan[],
   importPath: string,
   importOf: (buildSourceID: string) => string | undefined = () => undefined,
): { imports: string; sources: string } | undefined {
   if (plans.length === 0) return undefined;
   // One import per file, in the order each is first named.
   const byFile = new Map<string, Set<string>>();
   for (const p of plans) {
      const from = importOf(p.buildSourceID) ?? importPath;
      const names = byFile.get(from) ?? new Set<string>();
      names.add(p.queryName);
      if (p.sourceName !== undefined) names.add(p.sourceName);
      byFile.set(from, names);
   }
   const imports = [...byFile]
      .map(
         ([from, names]) =>
            `import { ${[...names]
               .sort(byCodepoint)
               .map((n) => identifier(n))
               .join(", ")} } from ${doubleQuotedString(from)}`,
      )
      .join("\n");
   const sources = plans
      .map(
         (p) =>
            // malloy-text: the author's own `#@` lines, verbatim (single lines, no `@env.`; see planModelPersistedQueries)
            `${p.persistLines.join("\n")}
source: ${identifier(p.buildSourceName)} is ${identifier(p.queryName)}`,
      )
      .join("\n\n");
   return { imports, sources };
}

/**
 * The persisted-result companion's text for one declaring file: its persisted
 * views' build sources, then its persisted queries', or undefined when it
 * declares neither.
 */
export function generatePersistedResultFile(
   views: readonly PersistedViewPlan[],
   queries: readonly PersistedQueryPlan[],
   importPath: string,
   /** Another file a build source's source is imported from, by build sourceID. */
   importOf?: (buildSourceID: string) => string | undefined,
): string | undefined {
   return generatePersistedViewFile(
      views,
      importPath,
      persistedQueryCompanionParts(queries, importPath, importOf),
      importOf,
   );
}

/** Appended to a declaring model's path to name its rollup build companion. */
export const ROLLUP_COMPANION_SUFFIX = ".preaggregate.malloy";

/**
 * The companion URL of the file a `sourceID` was declared in, or undefined
 * for a declaration that is not a local file. Appended at the URL, never
 * through a filesystem path, so it is the same URL on every platform.
 */
export function rollupCompanionURLOf(declaringURL: string): URL | undefined {
   let url: URL;
   try {
      url = new URL(declaringURL);
   } catch {
      return undefined;
   }
   if (url.protocol !== "file:") return undefined;
   url.pathname += ROLLUP_COMPANION_SUFFIX;
   return url;
}

/**
 * A `sourceID` with its file made relative to the package
 * (`orders@models/orders.malloy`), so a rollup's name does not depend on
 * where the package is checked out.
 */
export function packageRelativeSourceID(
   packagePath: string,
   sourceID: string,
): string {
   const at = splitSourceID(sourceID);
   if (!at) return sourceID;
   try {
      const file = fileURLToPath(at.url);
      const relative = path
         .relative(path.resolve(packagePath), file)
         .split(path.sep)
         .join("/");
      return `${at.name}@${relative}`;
   } catch {
      return sourceID;
   }
}

/** The alias a build file imports a base under. */
export function rollupBaseAlias(baseDeclaredName: string): string {
   return `${baseDeclaredName}__rollup_base`;
}

/** One rollup's `#@ persist` line. */
function persistLine(plan: RollupVariantPlan): string {
   if (plan.storage !== undefined) {
      return `#@ persist storage=${annotationValue(plan.storage)}`;
   }
   if (plan.namespace !== undefined) {
      return `#@ persist name=${annotationValue(`${plan.namespace}.${plan.rollupSourceName}`)}`;
   }
   return "#@ persist";
}

/** One rollup's build source. */
function emitRollupBuildSource(plan: RollupVariantPlan): string {
   const grain = plan.grainDimensions
      .map((g) => `    ${identifier(g)}`)
      .join("\n");
   const partials = plan.rollupMeasures
      .flatMap((m) => m.partials)
      .map(
         (p) =>
            `    ${identifier(p.column)} is ${emitMeasureExpr(p.leaf.expr)}`,
      )
      .join("\n");
   // A grain an `internal` gate field is in reads it through
   // `include { public: * }`, which lifts the fields the base hides from a
   // query; the SQL, and so the table's address, is the same.
   const base = identifier(rollupBaseAlias(plan.baseDeclaredName));
   const from =
      (plan.internalGrain ?? []).length > 0
         ? `${base} include { public: * }`
         : base;
   return `${persistLine(plan)}
source: ${identifier(plan.rollupSourceName)} is ${from} -> {
  group_by:
${grain}
  aggregate:
${partials}
}`;
}

/**
 * The build copy a rollup's base is imported from when its plan has one
 * (`RollupVariantPlan.buildCopy`): the base's declaring file, `importPath`,
 * with its given-reading terms left out and its hidden fields lifted, beside
 * it so its own imports resolve.
 */
export function rollupBuildCopyName(
   importPath: string,
   baseDeclaredName: string,
): string {
   return `${importPath}.${baseDeclaredName}.rollup-build.malloy`;
}

/**
 * The copy a rollup's `#(materialize)` base is imported from when the rollup
 * builds from the base's stored table (rollup_compile.ts): the base's
 * declaring file, `importPath`, with the base re-rooted on its generated
 * build source, beside it so its own imports resolve.
 */
export function rollupStoredBaseCopyName(
   importPath: string,
   baseDeclaredName: string,
): string {
   return `${importPath}.${baseDeclaredName}.rollup-stored-base.malloy`;
}

/** Whether a rollup's base is read from its build copy. */
export function readsBuildCopy(
   plan: Pick<RollupVariantPlan, "readTimeTerms" | "privateGrain">,
): boolean {
   return (
      (plan.readTimeTerms ?? []).length > 0 ||
      (plan.privateGrain ?? []).length > 0
   );
}

/**
 * A rollup base's build copy: `text`, its declaring file with every
 * source-level `#@` line blanked, with the base's given-reading terms
 * replaced by `true` (each found where the compiler says it is, {@link
 * termSpans}) and, with `lift`, its own access modifiers lifted and its
 * declaration wrapped in `include { public: * }` under the flag that needs
 * (as a `#(materialize)` build copy lifts them), and those of the sources
 * it extends that declare a private grain dimension (`liftedAncestors`).
 * Made the same by the plan's
 * companion and the serving variant, so the build source's SQL, and so its
 * table, is one. Or why it cannot be made.
 */
export function rollupBuildCopyText(
   text: string,
   plan: Pick<
      RollupVariantPlan,
      "readTimeTerms" | "declaration" | "liftedAncestors"
   >,
   lift: boolean,
): { copy: string } | { reason: string } {
   const edits: TextEdit[] = [];
   const terms = plan.readTimeTerms ?? [];
   if (terms.length > 0) {
      const found = termSpans(text, plan, terms);
      if ("reason" in found) return found;
      for (const span of found.spans) edits.push({ ...span, insert: "true" });
   }
   if (lift) {
      const range = plan.declaration?.range;
      const from = range ? offsetOf(text, range.start) : -1;
      const to = range ? offsetOf(text, range.end) : -1;
      const lifted =
         from >= 0 && to >= from
            ? accessLiftEdits(text.slice(from, to))
            : undefined;
      if (!lifted) {
         return {
            reason:
               "the source's declaration could not be read to lift its access modifiers for the build",
         };
      }
      edits.push(
         ...lifted.map((e) => ({
            ...e,
            start: from + e.start,
            end: from + e.end,
         })),
         { start: to, end: to, insert: " include { public: * }" },
         { start: 0, end: 0, insert: "##! experimental.access_modifiers\n" },
      );
      // A private grain dimension a source it extends declares: that
      // source's statement, and each between it and the base, lifted too,
      // so the field reaches the base public in the copy.
      for (const ancestor of plan.liftedAncestors ?? []) {
         const at = offsetOf(text, ancestor.range.start);
         const end = offsetOf(text, ancestor.range.end);
         const ancestorLifts =
            at >= 0 && end >= at
               ? accessLiftEdits(text.slice(at, end))
               : undefined;
         if (!ancestorLifts) {
            return {
               reason:
                  "the declaration of a source it extends could not be read to lift its access modifiers for the build",
            };
         }
         edits.push(
            ...ancestorLifts.map((e) => ({
               ...e,
               start: at + e.start,
               end: at + e.end,
            })),
         );
      }
   }
   const copy = applyEdits(text, edits);
   return copy === undefined ? { reason: TERM_NOT_FOUND } : { copy };
}

/**
 * The generated build companion for one declaring model, or undefined when it
 * declares no rollup. Every planned base is declared in that model, so each is
 * imported from it by `importPath`, or from its build copy beside it when its
 * plans have one, or, for a base named in `storedBases`, from the copy that
 * re-roots it on its stored table ({@link rollupStoredBaseCopyName}). The
 * experimental flag lives here, so the author's file needs none.
 */
export function generateRollupBuildFile(
   plans: readonly RollupVariantPlan[],
   importPath: string,
   storedBases: ReadonlySet<string> = new Set(),
): string | undefined {
   if (plans.length === 0) return undefined;
   const bases = [...new Set(plans.map((p) => p.baseDeclaredName))].sort(
      byCodepoint,
   );
   const copied = new Set(
      plans.filter(readsBuildCopy).map((p) => p.baseDeclaredName),
   );
   const imports = bases
      .map(
         (base) =>
            `import { ${identifier(rollupBaseAlias(base))} is ${identifier(base)} } from ${doubleQuotedString(copied.has(base) ? rollupBuildCopyName(importPath, base) : storedBases.has(base) ? rollupStoredBaseCopyName(importPath, base) : importPath)}`,
      )
      .join("\n");
   // `include { public: * }` (an internal gate field in a grain) needs the
   // access-modifier flag; the SQL, and so every table's address, is the same.
   const flags = plans.some((p) => (p.internalGrain ?? []).length > 0)
      ? "##! experimental { access_modifiers persistence }"
      : "##! experimental.persistence";
   return `${flags}
${imports}

${plans.map((p) => emitRollupBuildSource(p)).join("\n\n")}
`;
}

// ---------------------------------------------------------------------------
// Emission, through malloy_text
// ---------------------------------------------------------------------------

function binaryOpText(op: BinaryOp): string {
   switch (op) {
      case "+":
         return "+";
      case "-":
         return "-";
      case "*":
         return "*";
      case "/":
         return "/";
      case "%":
         return "%";
      case "=":
         return "=";
      case "!=":
         return "!=";
      case "<":
         return "<";
      case "<=":
         return "<=";
      case ">":
         return ">";
      case ">=":
         return ">=";
      case "~":
         return "~";
      case "!~":
         return "!~";
      case "and":
         return "and";
      case "or":
         return "or";
   }
}

function literalText(value: "true" | "false" | "null"): string {
   switch (value) {
      case "true":
         return "true";
      case "false":
         return "false";
      case "null":
         return "null";
   }
}

function aggregateMethodText(fn: string): string {
   switch (fn.toLowerCase()) {
      case "sum":
         return "sum";
      case "count":
         return "count";
      case "min":
         return "min";
      case "max":
         return "max";
      default:
         throw new Error(`not an aggregate the grammar emits: ${fn}`);
   }
}

function reaggregateText(r: Reaggregate): string {
   switch (r) {
      case "sum":
         return "sum";
      case "min":
         return "min";
      case "max":
         return "max";
   }
}

/** A field path, each segment through `identifier`. */
function pathText(path: readonly string[]): string {
   return path.map((segment) => identifier(segment)).join(".");
}

/** Malloy text for `e`, every name and value through `malloy_text`. */
export function emitMeasureExpr(e: MeasureExpr): string {
   switch (e.kind) {
      case "number":
         return numberLiteral(e.text);
      case "string":
         return stringLiteral(e.value);
      case "literal":
         return literalText(e.value);
      case "path":
         return pathText(e.path);
      case "call":
         return `${functionName(e.name)}(${e.args.map((a) => emitMeasureExpr(a)).join(", ")})`;
      case "method":
         return `${pathText(e.path)}.${aggregateMethodText(e.fn)}(${e.args.map((a) => emitMeasureExpr(a)).join(", ")})`;
      case "filtered":
         return `${emitMeasureExpr(e.expr)} { where: ${e.where.map((w) => emitMeasureExpr(w)).join(", ")} }`;
      case "unary": {
         const operand = emitMeasureExpr(e.expr);
         if (e.op === "not") return `not ${operand}`;
         // `--` starts a Malloy comment: keep two minus signs apart.
         return operand.startsWith("-") ? `- ${operand}` : `-${operand}`;
      }
      case "binary":
         return `${emitMeasureExpr(e.left)} ${binaryOpText(e.op)} ${emitMeasureExpr(e.right)}`;
      case "paren":
         return `(${emitMeasureExpr(e.expr)})`;
      case "partial":
         return `${identifier(e.column)}.${reaggregateText(e.reaggregate)}()`;
   }
}

/**
 * The notes of a view field the variant carries onto its own view: every
 * route but `#@`, inherited levels included (an alias `view: v2 is v`, or an
 * `include:` re-annotation, keeps the render tags of the view it names on an
 * inherited level, and live renders with them), ancestors first so a later
 * note overrides an earlier one as it does live.
 */
export function viewNoteTexts(
   annotations: AnnotationsDef | undefined,
): string[] {
   return (annotationTexts(annotations) ?? [])
      .map((t) => t.trim())
      .filter((t) => routeOfNote(t) !== "@" && !t.includes("\n"));
}
