// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the package warnings that name a deprecated format, so an
 * author knows to move off it. Each is one entry of
 * {@link deprecatedFormatWarnings}, which the package spreads into its
 * warnings at one call site; removing a format is deleting its entry here.
 */

import { components } from "../../api";
import { GIVENS_DOCS_URL } from "../../constants";
import type { Model } from "../model";
import { isMalloynbPath } from "./malloynb";

type ApiPackageWarning = NonNullable<
   components["schemas"]["Package"]["warnings"]
>[number];

/** `PackageWarning.code` of a `.malloynb` notebook's deprecated-format warning. */
export const DEPRECATED_MALLOYNB_NOTEBOOK = "deprecated-malloynb-notebook";
/** `PackageWarning.code` of a source's deprecated `#(filter)` annotations warning. */
export const DEPRECATED_FILTER_ANNOTATION = "deprecated-filter-annotation";

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

const DEPRECATED_LINTS: readonly DeprecatedLint[] = [
   malloynbNotebooks,
   filterAnnotations,
];

/** Every deprecated format's warnings, in registry order. */
export function deprecatedFormatWarnings(
   models: ReadonlyMap<string, Model>,
): ApiPackageWarning[] {
   return DEPRECATED_LINTS.flatMap((lint) => lint(models));
}
