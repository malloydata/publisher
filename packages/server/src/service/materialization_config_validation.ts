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
   /** The model path or source name to point the author at; absent for a package. */
   subject?: string;
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

/** One finding, in the shape the wire package's operator warnings array uses. */
export interface MaterializationConfigWarning {
   /** The persist source the finding belongs to, absent for a package-level one. */
   subject?: string;
   message: string;
}

function metadataWarnings(
   level: string,
   metadata: QueryMetadata | null | undefined,
   subject?: string,
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
      ...(subject ? { subject } : {}),
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
            declaration.subject,
         ),
      );
   }

   // Identical findings collapse (the same declaration reaching this function
   // twice must not produce two messages).
   const seen = new Set<string>();
   return warnings.filter((warning) => {
      const key = `${warning.subject ?? ""}\u0000${warning.message}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
   });
}
