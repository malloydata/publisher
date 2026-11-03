// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A published source whose `#(authorize)` or `#(access_filter)` reads a
 * given that is not declared `#(secure)` REFUSES the package load.
 *
 * A plain given is whatever the request supplies, so a gate keyed on one is a
 * filter the caller can set, with a policy-shaped name. A `#(secure)` given is
 * the host's to set: the trusted tier in front of Publisher resolves it from
 * the caller's verified identity and replaces anything the request carried
 * (docs/materialization.md, "Where the per-caller value comes from"). Only a
 * gate keyed on secure givens is a control, so a package that writes one on a
 * plain given does not load, the way a `#(carries)` leak or a join bypass does:
 * serving it would serve a surface that claims a boundary it does not have.
 * The message names the source and the givens to mark, once per source.
 */

import { referencedGivenNames } from "./authorize";
import type { Model } from "./model";

const isSentinel = (expr: string) =>
   ["false", "true"].includes(expr.trim().toLowerCase());

/**
 * Every given name the package declares `#(secure)`, in any model, listed or
 * not. Secure-ness is package-wide because the trusted tier reads it that way
 * (`Package.secureGivens`): it sets a name for the caller wherever the name is
 * read, so a gate that reads a secure given through an import, where the
 * given is off the entry model's own surface, is still keyed on a secure one.
 */
function secureGivenNames(models: ReadonlyMap<string, Model>): Set<string> {
   const names = new Set<string>();
   for (const model of models.values()) {
      let sources: ReturnType<Model["getSources"]>;
      try {
         sources = model.getSources();
      } catch {
         continue;
      }
      for (const source of sources ?? []) {
         for (const g of source.givens ?? []) {
            if (g.secure === true && g.name) names.add(g.name);
         }
      }
   }
   return names;
}

/** One rejection message per published source gated on a plain given; empty when none. */
export function insecureGateGivens(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
): string[] {
   const messages: string[] = [];
   const secure = secureGivenNames(models);
   for (const [modelPath, model] of models) {
      if (!isListed(modelPath)) continue;
      let apiSources: ReturnType<Model["getSources"]>;
      try {
         apiSources = model.getSources();
      } catch {
         continue;
      }
      for (const source of apiSources ?? []) {
         const read = new Set<string>();
         for (const expr of [
            ...(source.authorize ?? []),
            ...(source.accessFilter ?? []),
         ]) {
            if (isSentinel(expr)) continue;
            for (const name of referencedGivenNames(expr)) read.add(name);
         }
         const insecure = Array.from(read)
            .filter((name) => !secure.has(name))
            .sort();
         if (insecure.length === 0) continue;
         messages.push(
            `source ${source.name} (${modelPath}) is gated on $${insecure.join(", $")}, ` +
               `which ${insecure.length === 1 ? "is" : "are"} not declared ` +
               `#(secure), so a caller can set ${insecure.length === 1 ? "it" : "them"} ` +
               `and the gate is a filter, not a control. Write "#(secure)" on ` +
               `the given: line so the trusted tier in front of Publisher sets ` +
               `it from the caller's verified identity (docs/authorize.md, ` +
               `Security model).`,
         );
      }
   }
   return messages;
}
