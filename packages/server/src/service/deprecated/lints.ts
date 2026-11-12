// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the package warnings that name a deprecated format, so an
 * author knows to move off it. Each is one entry of
 * {@link deprecatedFormatWarnings}, which the package spreads into its
 * warnings at one call site, and each warning is also one use counted by
 * {@link recordDeprecatedFormatUsage}; removing a format is deleting its entry
 * here.
 */

import { components } from "../../api";
import { getMaterializeEnabled } from "../../config";
import {
   GIVENS_DOCS_URL,
   MATERIALIZATION_MIGRATION_DOCS_URL,
} from "../../constants";
import {
   recordDeprecatedUsage,
   type DeprecatedFeature,
} from "../../deprecated_usage_metrics";
import type { Model } from "../model";
import { isMalloynbPath } from "./malloynb";

type ApiPackageWarning = NonNullable<
   components["schemas"]["Package"]["warnings"]
>[number];

/** `PackageWarning.code` of a `.malloynb` notebook's deprecated-format warning. */
const DEPRECATED_MALLOYNB_NOTEBOOK = "deprecated-malloynb-notebook";
/** `PackageWarning.code` of a source's deprecated `#(filter)` annotations warning. */
const DEPRECATED_FILTER_ANNOTATION = "deprecated-filter-annotation";
/** `PackageWarning.code` of a source's deprecated `#@ -persist` opt-out warning. */
const DEPRECATED_PERSIST_OPT_OUT = "deprecated-persist-opt-out";
/** `PackageWarning.code` of a measure's deprecated `#@ preaggregate` warning. */
const DEPRECATED_PREAGGREGATE_ANNOTATION = "deprecated-preaggregate-annotation";

/** One deprecated format's warnings over a package's models. */
type DeprecatedLint = (
   models: ReadonlyMap<string, Model>,
) => ApiPackageWarning[];

const malloynbNotebooks: DeprecatedLint = (models) =>
   Array.from(models.keys())
      .filter(isMalloynbPath)
      .map((modelPath) => ({
         model: modelPath,
         code: DEPRECATED_MALLOYNB_NOTEBOOK,
         severity: "warn" as const,
         message:
            `${modelPath} is a .malloynb notebook, a deprecated format. ` +
            `Convert it to a .malloy report.`,
      }));

/**
 * One per source a file declares deprecated `#(filter)` annotations on, naming
 * that file (`model`) and the source (`subject`). A source that only inherits
 * a filter (an `extend`) or arrives by import is not reported: the fix is in
 * the declaring file, which is reported once
 * (`Model.declaredFilterSourceNames`). Hidden models are reported too: a filter
 * there is still deprecated syntax to migrate.
 */
const filterAnnotations: DeprecatedLint = (models) =>
   Array.from(models).flatMap(([modelPath, model]) =>
      model.declaredFilterSourceNames().map((name) => ({
         model: modelPath,
         subject: name,
         code: DEPRECATED_FILTER_ANNOTATION,
         severity: "warn" as const,
         message:
            `Source ${name} uses deprecated #(filter) annotations. Replace them ` +
            `with given: runtime parameters (see ${GIVENS_DOCS_URL}).`,
      })),
   );

/**
 * One per source a file declares `#@ -persist` on. Under materialization v2 a
 * materialized source serves every reader (P11), so the opt-out has no v2
 * form: the rewrite points the reader at a source with no annotation.
 */
const persistOptOuts: DeprecatedLint = (models) =>
   Array.from(models).flatMap(([modelPath, model]) =>
      model
         .declaredDeprecatedPersistAnnotations()
         .filter((found) => found.kind === "persist_opt_out")
         .map(({ source }) => ({
            model: modelPath,
            subject: source,
            code: DEPRECATED_PERSIST_OPT_OUT,
            severity: "warn" as const,
            message:
               `Source ${source} uses #@ -persist, which is deprecated: under ` +
               `#(materialize) a materialized source serves every reader, so ` +
               `there is nothing to opt out of. To keep ${source} live, base it ` +
               `on a source with no annotation, and put #(materialize) on a ` +
               `separate source that reads that same one (see ` +
               `${MATERIALIZATION_MIGRATION_DOCS_URL}). Until then it still ` +
               `recomputes live.`,
         })),
   );

/**
 * One per measure a file declares `#@ preaggregate` on (however many grains),
 * naming the source (`subject`) and the measure. Its v2 spelling is
 * `#(preaggregate) grain=[…]`, which materialization v2's phase 4 builds.
 */
const preaggregateAnnotations: DeprecatedLint = (models) =>
   Array.from(models).flatMap(([modelPath, model]) =>
      model
         .declaredDeprecatedPersistAnnotations()
         .flatMap((found) => (found.kind === "preaggregate" ? [found] : []))
         .map(({ source, measure }) => ({
            model: modelPath,
            subject: source,
            code: DEPRECATED_PREAGGREGATE_ANNOTATION,
            severity: "warn" as const,
            message:
               `Measure ${measure} on source ${source} uses #@ preaggregate, ` +
               `which is deprecated: its v2 spelling is #(preaggregate) ` +
               `grain=[…] (see ${MATERIALIZATION_MIGRATION_DOCS_URL}). ` +
               `#(preaggregate) is not built yet, so keep #@ preaggregate ` +
               `until it is; it still builds and serves its rollups.`,
         })),
   );

const DEPRECATED_LINTS: readonly {
   feature: DeprecatedFeature;
   lint: DeprecatedLint;
   /**
    * Whether the warning is shown. Usage is counted either way, as the
    * evidence for when to start warning. Absent: always shown.
    */
   shown?: () => boolean;
}[] = [
   { feature: "malloynb", lint: malloynbNotebooks },
   { feature: "filter_annotation", lint: filterAnnotations },
   // Materialization v2's deprecations start with the release that ships its
   // annotations (plan, question 5). Until then v2 is behind
   // `materialize.enabled`, so only a deployment that turned it on is told to
   // move to annotations it can use.
   {
      feature: "persist_opt_out",
      lint: persistOptOuts,
      shown: getMaterializeEnabled,
   },
   {
      feature: "preaggregate_annotation",
      lint: preaggregateAnnotations,
      shown: getMaterializeEnabled,
   },
];

/** Every deprecated format's warnings, in registry order. */
export function deprecatedFormatWarnings(
   models: ReadonlyMap<string, Model>,
): ApiPackageWarning[] {
   return DEPRECATED_LINTS.flatMap(({ lint, shown }) =>
      shown === undefined || shown() ? lint(models) : [],
   );
}

/**
 * Count each deprecated-format warning as one use of its feature: a
 * `.malloynb` file, a source declaring `#(filter)` or `#@ -persist`, or a
 * measure declaring `#@ preaggregate`, whether or not its warning is shown.
 * Called once per load
 * or reload; the warnings themselves are computed per request, so they are
 * not where to count.
 */
export function recordDeprecatedFormatUsage(
   models: ReadonlyMap<string, Model>,
): void {
   for (const { feature, lint } of DEPRECATED_LINTS) {
      recordDeprecatedUsage(feature, lint(models).length);
   }
}
