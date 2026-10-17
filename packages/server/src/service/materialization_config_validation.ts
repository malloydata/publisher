// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Publish-time validation of a package's materialization config, in one module
 * so the rules cannot drift between the levels a knob can be declared at.
 *
 *  1. **Nothing is silently dropped.** A declaration that will not do what it
 *     says surfaces somewhere: a publish error or warning, a 400, or a metered
 *     runtime drop.
 *  2. **Strict at declaration, lenient at execution.** Publish and the API
 *     report problems to a human; the runtime resolve path clamps and meters
 *     instead, because metadata must never break a query or a build.
 *
 * These findings are ADVISORY (the package's operator warnings array). They do
 * not go through `Package.persistencePolicyWarnings`, which rejects the publish
 * and disarms the scheduler: a deprecated manifest shape or a mistyped tag must
 * not stop a package from publishing.
 */

import {
   queryMetadataAdvisoryWarnings,
   queryMetadataBudgetWarning,
   queryMetadataReservedWarnings,
   queryMetadataViolations,
   type QueryMetadata,
} from "./query_metadata";

/** Where a `queryMetadata` bag was declared, which is what an author edits. */
type QueryMetadataLevel = "package" | "model" | "source";

/** One author's declaration, as declared — not as resolved. */
export interface QueryMetadataDeclaration {
   level: QueryMetadataLevel;
   /**
    * The source to point the author at; absent for a package or a model file.
    * Source names are unique only within a model, so a source declaration also
    * carries {@link modelPath}.
    */
   subject?: string;
   /** The model file the declaration is in; absent for a package. */
   modelPath?: string;
   queryMetadata: QueryMetadata;
}

interface MaterializationConfigInput {
   /**
    * Every `queryMetadata` bag an author declared, at whatever level, as
    * declared rather than resolved: a declaration answers "which line do I
    * edit", which is what makes a warning actionable. Any source can declare
    * one, persisted or not; it rides every query against that source.
    */
   declarations?: QueryMetadataDeclaration[];
   /**
    * How many properties a statement actually SENDS, per model file, after the
    * layers merge: each declaration can be under budget while their merge is
    * over it. Per model because `floorSize` (package ⊕ model file) is carried
    * by every source in the file, including sources that declare nothing.
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
 * One finding, in the shape of the package's operator warnings array. A model
 * path never goes in `subject`: clients filter findings by `model`.
 */
interface MaterializationConfigWarning {
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
 * attribution rather than corrupting anything. Each declaration is checked at
 * the level that declared it, so a message names the line to edit.
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

   // The budget applies to the merge a statement sends, which no single
   // declaration can answer (package 6 + model 6 is twelve on the wire). No
   // level label, because no single line is at fault.
   for (const merge of input.effectiveMerges ?? []) {
      // An overflowing floor is reported ALONE: every source carries it, and the
      // fix is in the manifest or model file, not the sources.
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

   // Identical findings collapse. Keyed on the model path too: a source name is
   // unique within a model, not within a package.
   const seen = new Set<string>();
   return warnings.filter((warning) => {
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
