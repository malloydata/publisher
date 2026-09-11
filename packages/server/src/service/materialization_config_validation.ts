// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Publish-time validation of a package's materialization config.
 *
 * One module for the rules so they cannot drift between the levels a knob can be
 * declared at. Two principles, both learned from shipped bugs:
 *
 *  1. **Nothing is silently dropped.** A declaration that will not do what it
 *     says has to surface somewhere — a publish error, a publish warning, a 400,
 *     or a metered runtime drop. A `#@ persist` name that was silently ignored
 *     (source published, never materialized, no error anywhere) is why
 *     `persist_annotation_validation.ts` exists; the same rule applies here.
 *  2. **Strict at declaration, lenient at execution.** Publish and the API have a
 *     human behind them and report problems; the runtime resolve path clamps and
 *     meters instead, because metadata must never break a query or a build.
 *
 * Today this owns the `queryMetadata` rules and the manifest-shape deprecations,
 * which are ADVISORY — they surface on the package's operator warnings array.
 * They deliberately do NOT go through `Package.persistencePolicyWarnings`, which
 * is a publish REJECTION gate (and disarms the scheduler): a deprecated manifest
 * shape or a mistyped tag must not stop a package from publishing. The
 * scope/schedule/freshness coherence rules that do reject still live there;
 * moving them here is a mechanical follow-up with no behavior change.
 */

import {
   queryMetadataAdvisoryWarnings,
   queryMetadataBudgetWarning,
   queryMetadataReservedWarnings,
   queryMetadataViolations,
   type QueryMetadata,
} from "./query_metadata";

/** Where a `queryMetadata` bag was declared, which is what an author edits. */
export type QueryMetadataLevel = "package" | "model" | "source";

/** One author's declaration, as declared — not as resolved. */
export interface QueryMetadataDeclaration {
   level: QueryMetadataLevel;
   /**
    * The source to point the author at; absent for a package or a model file.
    * A source name is not unique across a package, so a source declaration
    * carries {@link modelPath} as well — without it two models declaring a
    * same-named source produce identical findings that dedupe to one message
    * naming neither file.
    */
   subject?: string;
   /** The model file the declaration is in; absent for a package. */
   modelPath?: string;
   queryMetadata: QueryMetadata;
}

export interface MaterializationConfigInput {
   /**
    * Every `queryMetadata` bag an author declared, at whatever level.
    *
    * One list rather than one field per level, and OWN declarations rather than
    * resolved ones. A resolved bag answers "what will this source send"; a
    * declaration answers "which line do I edit", and only the second is
    * actionable in a warning. Reporting resolved bags is also what made a
    * package property surface under a `#@ persist` label at a source that never
    * mentioned it.
    *
    * Whether a source persists is irrelevant here: `queryMetadata` is a sibling
    * of `persist` in the `#@` namespace, so any source can declare a bag and it
    * rides every query against that source either way.
    */
   declarations?: QueryMetadataDeclaration[];
   /**
    * How many properties a statement actually SENDS, per model file, after the
    * layers merge. The declarations above cannot answer this — each is
    * individually under budget while their merge is over it — and the budget is
    * a property of the merge.
    *
    * Given per model rather than per source because the overflow usually is not
    * a source's fault. `floorSize` is package ⊕ model file: the bag EVERY source
    * in that file carries, including the sources that declare nothing. Sizing
    * only the declaring sources missed that case entirely — a package and a
    * model file that overflow together published clean whenever no source in the
    * file happened to add a tag of its own.
    */
   effectiveMerges?: {
      modelPath: string;
      /** package ⊕ model file — carried by every source in this file. */
      floorSize: number;
      /** Per source that declares its own bag: package ⊕ model ⊕ source. */
      sources: { subject: string; size: number }[];
   }[];
   /**
    * Deprecations the manifest parse tolerated (e.g. a root-level `scope`), which
    * a load keeps working but a publish should report.
    */
   manifestWarnings?: string[];
}

/**
 * How each level is spelled back to the author. The canonical spelling, not the
 * deprecated `materialization.` one — a message should name the form we want
 * them to write.
 */
const DECLARATION_LABELS: Record<QueryMetadataLevel, string> = {
   package: "queryMetadata",
   model: "## queryMetadata",
   source: "#@ queryMetadata",
};

/**
 * One finding, in the shape the wire package's operator warnings array uses.
 *
 * `model` and `subject` are separate because the schema separates them: `model`
 * is a package-relative model path and `subject` is a source / query / view.
 * Putting a model path in `subject` made a client filtering by `model` miss the
 * finding entirely, and rendered `marts.malloy` where a reader expects a source
 * name.
 */
export interface MaterializationConfigWarning {
   /** The model file the finding is in, absent for a package-level one. */
   model?: string;
   /** The source the finding belongs to, absent for package and model levels. */
   subject?: string;
   message: string;
}

function metadataWarnings(
   level: string,
   metadata: QueryMetadata | null | undefined,
   location: { model?: string; subject?: string },
): MaterializationConfigWarning[] {
   if (!metadata) return [];
   const budget = queryMetadataBudgetWarning(Object.keys(metadata).length);
   return [
      ...queryMetadataViolations(metadata),
      ...queryMetadataAdvisoryWarnings(metadata),
      ...queryMetadataReservedWarnings(metadata),
      ...(budget ? [budget] : []),
   ].map((message) => ({
      // The level is in the message because a reader needs to know WHICH
      // declaration to edit, and an inherited property has more than one.
      message: `${level}: ${message}`,
      ...(location.model ? { model: location.model } : {}),
      ...(location.subject ? { subject: location.subject } : {}),
   }));
}

/**
 * Everything wrong or inadvisable about a package's materialization config, as
 * actionable messages. Warnings rather than errors: a package published before a
 * rule existed must keep loading, and a bad metadata property degrades
 * attribution rather than corrupting anything.
 *
 * Each declaration is checked as DECLARED, at the level that declared it, so a
 * message names the line an author can edit. Checking a source's resolved bag
 * instead would report a package property at every source that inherits it,
 * under a `#@ persist` label those sources never wrote.
 */
export function materializationConfigWarnings(
   input: MaterializationConfigInput,
): MaterializationConfigWarning[] {
   const warnings: MaterializationConfigWarning[] = (
      input.manifestWarnings ?? []
   ).map((message) => ({ message }));

   for (const declaration of input.declarations ?? []) {
      warnings.push(
         ...metadataWarnings(
            DECLARATION_LABELS[declaration.level],
            declaration.queryMetadata,
            { model: declaration.modelPath, subject: declaration.subject },
         ),
      );
   }

   // The budget is the one rule a single declaration cannot answer. Every check
   // above is per-declaration on purpose — that is what lets a message name the
   // line to edit — but the budget is about what a statement SENDS, and a
   // statement sends the merge of every layer above it. Package 6 + model 6 is
   // six properties at each declaration and twelve on the wire, which is over
   // the author budget and sheds context properties on every statement: the
   // exact failure `queryMetadataBudgetWarning` exists to prevent, published
   // clean and visible afterwards only as a metric.
   //
   // No level label on these, because no single line is at fault.
   for (const merge of input.effectiveMerges ?? []) {
      // The floor first, and ALONE when it overflows. Every source in the file
      // carries it, so naming each would be N messages for one mistake — and the
      // sources are not where the fix goes, the manifest and this file are. Once
      // the floor is back under budget a republish surfaces whichever individual
      // sources are still over.
      const floor = queryMetadataBudgetWarning(merge.floorSize);
      if (floor) {
         warnings.push({
            model: merge.modelPath,
            message: `queryMetadata (package + model file, merged): ${floor}`,
         });
         continue;
      }
      for (const { subject, size } of merge.sources) {
         const budget = queryMetadataBudgetWarning(size);
         if (!budget) continue;
         warnings.push({
            model: merge.modelPath,
            subject,
            message: `queryMetadata (package + model file + source, merged): ${budget}`,
         });
      }
   }

   // Identical findings collapse (the same declaration reaching this function
   // twice must not produce two messages).
   const seen = new Set<string>();
   return warnings.filter((warning) => {
      // Keyed on the model path as well as the source: a source name is
      // unique within a model, not within a package, so keying on the name
      // alone collapsed two files' findings into one message naming neither.
      const key = [
         warning.model ?? "",
         warning.subject ?? "",
         warning.message,
      ].join("\u0000");
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
   });
}
