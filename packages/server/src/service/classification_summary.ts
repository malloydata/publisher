// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `Package.classification`: for each published entry point, the classes it
 * declares in `#(carries)` and how many of its fields carry each class. It is
 * what a scorecard or a CI check needs to say "this domain may serve X and
 * serves Y" without walking every field, and it covers only the sources
 * discovery already shows, so it reveals nothing curation hides. A field's
 * own classes, with what they were inferred from, are on its `get_context`
 * card.
 *
 * Read from the same published-source walk as the `classification-coverage`
 * warning ({@link publishedSources}), so the two never disagree. A source
 * locked to every caller publishes nothing and is left out, as coverage
 * leaves it out.
 */

import type { components } from "../api";
import { publishedSources, readCarries } from "./classification";
import type { Model } from "./model";

export type ApiClassificationSummary =
   components["schemas"]["ClassificationSummary"];
type ApiClassificationSource =
   components["schemas"]["ClassificationSummarySource"];

const byCodepoint = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/** A plain object with its keys in codepoint order, for stable output. */
function sortedCounts(counts: Map<string, number>): Record<string, number> {
   const out: Record<string, number> = {};
   for (const key of Array.from(counts.keys()).sort(byCodepoint)) {
      out[key] = counts.get(key) as number;
   }
   return out;
}

/**
 * The summary over a package's published models, or `undefined` when no
 * field of any published source carries a class: a package that classifies
 * nothing has nothing to summarize.
 */
export function classificationSummary(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
): ApiClassificationSummary | undefined {
   const sources: ApiClassificationSource[] = [];
   let classifiesFields = false;
   const published = publishedSources(models, isListed)
      .filter((s) => !s.deniedToAll)
      .sort(
         (a, b) =>
            byCodepoint(a.modelPath, b.modelPath) ||
            byCodepoint(a.source, b.source),
      );
   for (const s of published) {
      const counts = new Map<string, number>();
      let unclassified = 0;
      for (const [, field] of s.fields) {
         if (field.classifications.length === 0) {
            unclassified += 1;
            continue;
         }
         classifiesFields = true;
         for (const c of field.classifications) {
            counts.set(c, (counts.get(c) ?? 0) + 1);
         }
      }
      const carries = readCarries(s.ir.annotations);
      sources.push({
         modelPath: s.modelPath,
         name: s.source,
         ...(carries.declared
            ? { carries: Array.from(carries.classes).sort(byCodepoint) }
            : {}),
         fields: s.fields.length,
         unclassified,
         classCounts: sortedCounts(counts),
      });
   }
   return classifiesFields ? { sources } : undefined;
}
