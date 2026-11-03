// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's `#(secure)` givens, summarized from what its load already
 * compiled, so a host that sets those values does not compile the package a
 * second time to learn which ones they are.
 */

import { components } from "../api";
import { MODEL_FILE_SUFFIX } from "../constants";

export type ApiSecureGivenSummary = components["schemas"]["SecureGivenSummary"];

/** One loaded model file, as the summary reads it. */
export interface SecureGivenModelInput {
   /** Package-relative path of the model file. */
   modelPath: string;
   /** The givens its compiled model surfaces (`CompiledModel.givens`). */
   givens: readonly Pick<
      components["schemas"]["Given"],
      "name" | "type" | "secure"
   >[];
   /** False for a placeholder kept for a file that failed to compile. */
   compiled: boolean;
}

/** Code-unit order: deterministic, unlike a locale-aware compare. */
function compareText(a: string, b: string): number {
   return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * One entry per distinct (name, type) among the `#(secure)` givens of every
 * compiled `.malloy` model, naming the models that surface it. Notebooks are
 * left out: a `.malloynb` is a reader, not a model a host binds a given for.
 * Entries are sorted by name then type, and each `modelPaths` is sorted and
 * deduplicated, so the same load always yields the same array.
 */
export function summarizeSecureGivens(
   models: Iterable<SecureGivenModelInput>,
): ApiSecureGivenSummary[] {
   const byKey = new Map<
      string,
      { name: string; type: string; modelPaths: Set<string> }
   >();
   for (const model of models) {
      if (!model.compiled) continue;
      if (!model.modelPath.endsWith(MODEL_FILE_SUFFIX)) continue;
      for (const given of model.givens) {
         // `secure` is `#(secure)` route presence (see `malloyGivenToApi`).
         if (given.secure !== true || !given.name || !given.type) continue;
         // NUL cannot appear in a Malloy identifier or a rendered type.
         const key = `${given.name}\u0000${given.type}`;
         let entry = byKey.get(key);
         if (!entry) {
            entry = {
               name: given.name,
               type: given.type,
               modelPaths: new Set(),
            };
            byKey.set(key, entry);
         }
         entry.modelPaths.add(model.modelPath);
      }
   }
   return [...byKey.values()]
      .sort(
         (a, b) => compareText(a.name, b.name) || compareText(a.type, b.type),
      )
      .map(({ name, type, modelPaths }) => ({
         name,
         type,
         modelPaths: [...modelPaths].sort(compareText),
      }));
}
