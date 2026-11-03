// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's `#(secure)` givens, summarized from what its load already
 * compiled, so a host that sets those values does not compile the package a
 * second time to learn which ones they are.
 */

import { components } from "../api";
import { compareText } from "../compare_text";
import { logger } from "../logger";

export type ApiSecureGivenSummary = components["schemas"]["SecureGivenSummary"];

/** One loaded model or notebook file, as the summary reads it. */
export interface SecureGivenModelInput {
   /** Package-relative path of the `.malloy` model or `.malloynb` notebook. */
   modelPath: string;
   /**
    * The givens its compiled model surfaces (`CompiledModel.givens`); for a
    * notebook, those of the model its cells build up to.
    */
   givens: readonly Pick<
      components["schemas"]["Given"],
      "name" | "type" | "secure"
   >[];
   /** False for a placeholder kept for a file that failed to compile. */
   compiled: boolean;
}

/**
 * One entry per distinct (name, type) among the `#(secure)` givens of every
 * model and notebook in the load, naming the files that surface it. Entries
 * are sorted by name then type, and each `modelPaths` is sorted and
 * deduplicated, so the same load always yields the same array. The result is
 * frozen, nested arrays and entries included, so it can be shared by
 * reference.
 *
 * `undefined` when the answer is not known to be complete: a file that failed
 * to compile, or a secure given with no name or type. A host must read that as
 * unknown, never as a short list.
 */
export function summarizeSecureGivens(
   models: Iterable<SecureGivenModelInput>,
   context: { packageName?: string } = {},
): readonly ApiSecureGivenSummary[] | undefined {
   const byKey = new Map<
      string,
      { name: string; type: string; modelPaths: Set<string> }
   >();
   for (const model of models) {
      // A failed file's givens are unknown, so the package's are too.
      if (!model.compiled) return undefined;
      for (const given of model.givens) {
         // `secure` is `#(secure)` route presence (see `malloyGivenToApi`).
         if (given.secure !== true) continue;
         if (!given.name || !given.type) {
            logger.warn(
               "A #(secure) given has no name or type; the package's secureGivens is left unreported",
               {
                  packageName: context.packageName,
                  modelPath: model.modelPath,
                  name: given.name,
                  type: given.type,
               },
            );
            return undefined;
         }
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
   return Object.freeze(
      [...byKey.values()]
         .sort(
            (a, b) =>
               compareText(a.name, b.name) || compareText(a.type, b.type),
         )
         .map(({ name, type, modelPaths }) =>
            Object.freeze({
               name,
               type,
               modelPaths: Object.freeze([...modelPaths].sort(compareText)),
            }),
         ),
   ) as readonly ApiSecureGivenSummary[];
}
