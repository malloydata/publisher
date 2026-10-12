// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: what `explores` in publisher.json contributes to the report
 * checks. `explores` lists files that make up the curated surface beside, or
 * instead of, the root `index.malloy`. The report lint reads the surface from
 * `index.malloy`'s imports and asks this module for the rest, so dropping
 * `explores` is deleting this module and its two calls in `Package`.
 */

import { INDEX_MODEL_NAME, MODEL_FILE_SUFFIX } from "../../constants";

/** The importer a file `explores` lists is reported under. */
export const EXPLORES_IMPORTER = "explores";

/** The files `explores` puts on the surface, other than `index.malloy` itself. */
export function exploresSurfaceFiles(
   explores: readonly string[] | undefined,
): string[] {
   return (explores ?? []).filter((entry) => entry !== INDEX_MODEL_NAME);
}

/** `report-on-surface` for a report `explores` lists. */
export function reportListedInExploresMessage(modelPath: string): string {
   return (
      `${modelPath} is listed in "explores" but is a report (it carries an ` +
      `artifact tag), so it adds nothing to the surface: a report reads the ` +
      `surface and never adds to it. Fix: remove it from "explores", or remove ` +
      `its artifact tag if it is meant to be a model.`
   );
}

/**
 * How a tile's surface refusal is explained when `explores` is the surface:
 * the fix goes through a file it lists, and when it lists only reports, no
 * listed file can export anything, so the working fix is to let index.malloy
 * be the surface. `source` is absent when the query route's refusal would not
 * name it (a gated model), and then neither does the warning.
 */
export function exploresSurfaceFix(
   explores: readonly string[] | undefined,
   isReport: (modelPath: string) => boolean,
): { unexported: string; fixFor: (source: string | undefined) => string } {
   const listedModel = (explores ?? []).find(
      (entry) => entry.endsWith(MODEL_FILE_SUFFIX) && !isReport(entry),
   );
   return {
      unexported: `which no file "explores" lists exports`,
      fixFor: (source) => {
         const name = source ?? "the source it reads";
         return listedModel
            ? `Fix: add ${name} to the export { ... } in ${listedModel}.`
            : `Fix: delete "explores" from publisher.json and add ${name} ` +
                 `to the export { ... } in ${INDEX_MODEL_NAME}.`;
      },
   };
}
