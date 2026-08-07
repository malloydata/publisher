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

import type { components } from "../api";
import {
   queryMetadataAdvisoryWarnings,
   queryMetadataBudgetWarning,
   queryMetadataViolations,
   type QueryMetadata,
} from "./query_metadata";

type WirePackageMaterialization =
   components["schemas"]["PackageMaterializationConfig"];
type WirePersistSourcePlan = components["schemas"]["PersistSourcePlan"];

export interface MaterializationConfigInput {
   /** The package manifest's `materialization` block, as parsed. */
   packageMaterialization?: WirePackageMaterialization | null;
   /** The compiled plan's sources, carrying each one's RESOLVED metadata. */
   sources?: WirePersistSourcePlan[];
   /**
    * Deprecations the manifest parse tolerated (e.g. a root-level `scope`), which
    * a load keeps working but a publish should report.
    */
   manifestWarnings?: string[];
}

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
 * A source's metadata is checked in its RESOLVED form (the effective bag from
 * package → model-file → `#@ persist`), because that is what will actually be
 * attached — a property inherited from the manifest is just as broken at the
 * source as it is at the package, and reporting it at both is how an author finds
 * the one they can edit.
 */
export function materializationConfigWarnings(
   input: MaterializationConfigInput,
): MaterializationConfigWarning[] {
   const warnings: MaterializationConfigWarning[] = (
      input.manifestWarnings ?? []
   ).map((message) => ({ message }));

   warnings.push(
      ...metadataWarnings(
         "materialization.queryMetadata",
         input.packageMaterialization?.queryMetadata,
      ),
   );

   for (const source of input.sources ?? []) {
      warnings.push(
         ...metadataWarnings(
            `#@ persist queryMetadata`,
            source.queryMetadata,
            source.name,
         ),
      );
   }

   // Identical findings collapse (two sources inheriting one bad package
   // property produce one message each, not one per level per source).
   const seen = new Set<string>();
   return warnings.filter((warning) => {
      const key = `${warning.subject ?? ""}\u0000${warning.message}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
   });
}
