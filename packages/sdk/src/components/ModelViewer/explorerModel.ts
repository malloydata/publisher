// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import { parseSourceFilterAnnotations } from "./modelGivens";

/**
 * Each source's required filters, by source name, for the sources that declare
 * any. Only the annotations: a required filter is declared there, and the field
 * walk the full metadata does is for filter controls this surface doesn't render.
 */
export function requiredFiltersBySource(
   sourceInfos: Malloy.SourceInfo[],
): Map<string, string[]> {
   const out = new Map<string, string[]>();
   for (const info of sourceInfos) {
      const required = parseSourceFilterAnnotations(info.annotations)
         .filter((f) => f.required && !f.implicit)
         .map((f) => f.name);
      if (required.length > 0) out.set(info.name, required);
   }
   return out;
}

/** The names of the model's sources: what lineage, inherited givens and a
 *  same-model join target are resolved against. */
export function sourceNameSet(
   sources: readonly { name: string }[],
): ReadonlySet<string> {
   return new Set(sources.map((s) => s.name));
}
