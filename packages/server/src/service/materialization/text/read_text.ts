// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The Malloy that declares a source over a stored table: a persisted view's
 * build companion, a rollup's build companion and its merged measures, the
 * companion URLs beside a declaring file, and the notes a variant carries
 * onto a view it redeclares.
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
import { fileURLToPath, pathToFileURL } from "url";
import { annotationTexts, type AnnotationsDef } from "../../annotations";
import { byCodepoint, routeOfNote } from "../../annotation_reader";
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
   splitSourceID,
   type PersistedViewPlan,
   type RollupVariantPlan,
} from "../domain/table_plans";

/** Appended to a declaring file's URL to name its persisted-view companion. */
export const PERSISTED_VIEW_COMPANION_SUFFIX = ".persist-view.malloy";

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
 * imported as `importPath`, or undefined when there are none.
 */
export function generatePersistedViewFile(
   plans: readonly PersistedViewPlan[],
   importPath: string,
): string | undefined {
   if (plans.length === 0) return undefined;
   const sources = [...new Set(plans.map((p) => p.sourceName))].sort(
      byCodepoint,
   );
   const imports = sources
      .map(
         (s) =>
            `import { ${identifier(baseAlias(s))} is ${identifier(s)} } from ${doubleQuotedString(importPath)}`,
      )
      .join("\n");
   const sourcesText = plans
      .map(
         (p) =>
            // malloy-text: the author's own `#@` lines, verbatim (single lines, no `@env.`; see planSourcePersistedViews)
            `${p.persistLines.join("\n")}
source: ${identifier(p.buildSourceName)} is ${identifier(baseAlias(p.sourceName))} -> ${identifier(p.viewName)}`,
      )
      .join("\n\n");
   return `##! experimental.persistence
${imports}

${sourcesText}
`;
}

/** Appended to a declaring model's path to name its rollup build companion. */
export const ROLLUP_COMPANION_SUFFIX = ".preaggregate.malloy";

/**
 * The companion URL of the file a `sourceID` was declared in, or undefined
 * for a declaration that is not a local file.
 */
export function rollupCompanionURLOf(declaringURL: string): URL | undefined {
   try {
      return pathToFileURL(
         `${fileURLToPath(declaringURL)}${ROLLUP_COMPANION_SUFFIX}`,
      );
   } catch {
      return undefined;
   }
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
   // query (A4); the SQL, and so the table's address, is the same.
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
 * The generated build companion for one declaring model, or undefined when it
 * declares no rollup. Every planned base is declared in that model, so each is
 * imported from it by `importPath`. The experimental flag lives here, so the
 * author's file needs none.
 */
export function generateRollupBuildFile(
   plans: readonly RollupVariantPlan[],
   importPath: string,
): string | undefined {
   if (plans.length === 0) return undefined;
   const bases = [...new Set(plans.map((p) => p.baseDeclaredName))].sort(
      byCodepoint,
   );
   const imports = bases
      .map(
         (base) =>
            `import { ${identifier(rollupBaseAlias(base))} is ${identifier(base)} } from ${doubleQuotedString(importPath)}`,
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
